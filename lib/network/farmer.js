'use strict';

const async = require('async');
const assert = require('assert');
const secp256k1 = require('secp256k1');
const https = require('https');
const http = require('http');
const crypto = require('crypto');
const kfs = require('kfs');
const path = require('path');
const kad = require('kad');
const Network = require('./');
const url = require('url');
const inherits = require('util').inherits;
const StorageItem = require('../storage/item');
const Contract = require('../contract');
const merge = require('merge');
const constants = require('../constants');
const diskusage = require('diskusage');
const utils = require('../utils');
const {execFile} = require('child_process');

/**
 * Creates and a new farmer interface
 * @constructor
 * @license AGPL-3.0
 * @extends {Network}
 * @param {Object} options
 * @param {String} options.paymentAddress - Optional payment address
 * @param {Array} options.opcodeSubscriptions - Contract opcodes to farm
 * @param {FarmerInterface~negotiator} options.contractNegotiator
 * @param {KeyPair} options.keyPair - Node's cryptographic identity
 * @param {StorageManager} options.storageManager - Storage manager backend
 * @param {String} options.bridges - An array of bridges to connect and accept contracts
 * @param {Object} options.logger - Logger instance
 * @param {Array} options.seedList - List of seed URIs to join
 * @param {String} options.rpcAddress - Public node IP or hostname
 * @param {Number} options.rpcPort - Listening port for RPC
 * @param {Boolean} options.doNotTraverseNat - Skip NAT traversal strategies
 * @param {Number} options.maxTunnels - Max number of tunnels to provide
 * @param {Number} options.tunnelServerPort - Port for tunnel server to use
 * @param {Object} options.tunnelGatewayRange
 * @param {Number} options.tunnelGatewayRange.min - Min port for gateway binding
 * @param {Number} options.tunnelGatewayRange.max - Max port for gateway binding
 * @param {Number} [options.offerBackoffLimit=4] - Do not send offers if more
 * than N transfers are active
 * @param {Object} [options.joinRetry]
 * @param {Number} [options.joinRetry.times] - Times to retry joining net
 * @param {Number} [options.joinRetry.interval] - MS to wait before retrying
 * @param {Number} [options.maxShardSize] - Max number of bytes to allow as contract shard size
 * @emits Network#ready
 * @property {KeyPair} keyPair
 * @property {StorageManager} storageManager
 * @property {kad.Node} node - The underlying DHT node
 * @property {TriggerManager} triggerManager
 * @property {Contact} contact
 * @property {Transport} transportAdapter
 * @property {kad.Router} router - The underlying DHT router
 * @property {DataChannelServer} dataChannelServer
 */
// eslint-disable-next-line max-statements
function FarmerInterface(options) {
  if (!(this instanceof FarmerInterface)) {
    return new FarmerInterface(options);
  }

  options = merge.recursive(Object.create(FarmerInterface.DEFAULTS), options);

  this._contractCount = 0;
  this._dataReceivedCount = 0;
  this._negotiator = options.contractNegotiator;
  this._offerBackoffLimit = options.offerBackoffLimit;

  this._maxShardSize = options.maxShardSize || constants.MAX_SHARD_SIZE;

  assert(options.storagePath, 'storagePath is expected option');

  Network.call(this, options);

  // Give warnings for deprecated configuration options
  this._deprecatedConfigV1(options);

  this._mapBridges(this._options.bridges);
  this._connectBridgesRunnning = false;
  this._connectBridgesInterval = null;

  this.spaceAvailable = true;
  if (options.spaceAvailable !== undefined) {
    this.spaceAvailable = options.spaceAvailable;
  }

  // Connecting the dots. Note: We should organize this better
  // so that it's not necesarry to set these after they
  // have been constructed.
  this._protocol.handleAlloc = this.handleAlloc.bind(this);
  this.transport.shardServer.farmerInterface = this;

}

inherits(FarmerInterface, Network);

FarmerInterface.CONNECT_BRIDGE_INTERVAL = 10000;
FarmerInterface.CHECK_SPACE_USED_INTERVAL = 86400000;
FarmerInterface.FREE_SPACE_PADDING = constants.FREE_SPACE_PADDING;

FarmerInterface.prototype._deprecatedConfigV1 = function(options) {
  if (options.renterWhitelist) {
    this._logger.warn('config renterWhitelist is deprecated, ' +
                      'please use bridges config instead');
  }

  if (options.bridgeUri) {
    this._logger.warn('config bridgeUri is deprecated, please ' +
                      'use bridges config instead');
  }

  if (options.renterWhitelist && options.bridges) {
    this._logger.warn('config renterWhitelist and bridges are mutually ' +
                      'exclusive, bridges config used only');
  }

  if (options.bridgeUri && options.bridges) {
    this._logger.warn('config bridgeUri and bridges are mutually exclusive, ' +
                      'bridges config used only');
  }
}

FarmerInterface.prototype.isBridgeConnected = function(contract) {
  if (!this.bridges) {
    this._logger.debug('no bridges are connected');
    return false;
  }
  var isConnected = this.bridges.has(contract.get('renter_hd_key'));
  this._logger.debug('contract is associated with connected bridge: %s',
                     isConnected);
  if (!isConnected) {
    return false;
  }
  return true;
}

/**
 * Called when a contract is found that meets subscription criteria and allows
 * us to modify the contract terms if we desire and then uses the return value
 * to determine if we should send the renter an offer
 * @callback FarmerInterface~negotiator
 * @param {Contract} contract - The contract object to negotiate
 * @returns {Boolean}
 */

// eslint-disable-next-line max-statements
FarmerInterface.Negotiator = function(contract, callback) {
  /* eslint complexity: [2, 7] */
  var self = this;
  const dataSize = contract.get('data_size');

  if (this._maxShardSize > 0 && this._maxShardSize < dataSize) {
    return callback(false);
  }

  if (!this.isBridgeConnected(contract)) {
    return callback(false);
  }

  if (!contract.get('data_hash')) {
    self._logger.warn('contract received with invalid data_hash, ignoring');
    return callback(false);
  }

  // NB: Backoff on sending offers if we are already have high active transfer
  var concurrentTransfer = (
    self.transport.shardServer.activeTransfers >= self._offerBackoffLimit
  );
  self._logger.debug(
    'active transfers %s is less than offerBackoffLimit %s: %s',
    self.transport.shardServer.activeTransfers,
    self._offerBackoffLimit,
    !concurrentTransfer
  );
  if (concurrentTransfer) {
    self._logger.warn('too many active transfers, not sending offer');
    return callback(false);
  }

  // Check that there is disk space available
  // TODO: This can be removed when there is better shard usage tracking
  diskusage.check(this._options.storagePath, (err, info) => {
    if (err) {
      self._logger.warn('unable to check disk usage for free space');
      return callback(false);
    }

    if (info.available - dataSize <= FarmerInterface.FREE_SPACE_PADDING) {
      self._logger.warn('disk space is at maximum capacity');
      // We can set noSpaceLeft here as we've guarded against
      // large shards with a maxShardSize check
      self.noSpaceLeft(true);
      return callback(false);
    }

    // NB: Only bid on contracts for data we don't have
    this.storageManager.load(contract.get('data_hash'), function(err, item) {
      if (err) {
        self._logger.debug('no storage item available for this shard');
        return callback(true);
      }

      var renters = Object.keys(item.contracts);

      if (renters.indexOf(contract.get('renter_id')) === -1) {
        self._logger.debug('no contract currently staged for this shard');
        return callback(true);
      }

      if (typeof item.shard.write === 'function') {
        self._logger.debug('no data currently stored for this shard');
        return callback(true);
      }

      self._logger.debug('shard already stored, not sending offer');
      callback(false);
    });

  });

};

FarmerInterface.DEFAULTS = {
  paymentAddress: '',
  opcodeSubscriptions: ['0f01020202', '0f02020202', '0f03020202'],
  contractNegotiator: FarmerInterface.Negotiator,
  offerBackoffLimit: 4
};

/**
 * Wraps the super call to {@link Network#join} to listen for contract after
 * successfully establishing a connection to the network
 * @param {Function} callback - Called on successful join
 */
FarmerInterface.prototype.join = function(callback) {
  var self = this;

  Network.prototype.join.call(this, function(err) {
    if (err) {
      return callback(err);
    }

    self._listenForContracts(self._options.opcodeSubscriptions);
    self.on(
      'connected',
      self._listenForContracts.bind(self, self._options.opcodeSubscriptions)
    );

    /* istanbul ignore next*/
    self.transport.shardServer.on('shardUploaded', function(){
      if (self._dataReceivedCount < Number.MAX_SAFE_INTEGER) {
        self._dataReceivedCount++;
      } else {
        self._dataReceivedCount = 0;
      }
    });

    callback();
  });
};

FarmerInterface.prototype._mapBridges = function(bridges) {
  this.bridges = new Map();
  for (let i = 0; i < bridges.length; i++) {
    this.bridges.set(bridges[i].extendedKey, {
      url: bridges[i].url,
      extendedKey: bridges[i].extendedKey,
      connected: false
    });
  }
};

FarmerInterface.prototype._connectBridges = function() {
  if (this._connectBridgesRunning) {
    return;
  }
  this._connectBridgesRunning = true;
  this.emit('bridgesConnecting');
  async.eachSeries(this.bridges.values(), (bridge, next) => {
    if (bridge.connected) {
      return next();
    }
    this.emit('bridgeConnecting', bridge);
    this._connectBridge(bridge, (err) => {
      if (err) {
        this._logger.error('Unable to connect to bridge: %s, reason: %s',
                           bridge.url, err.message);
        return next();
      }
      this.bridges.get(bridge.extendedKey).connected = true;
      this.emit('bridgeConnected', bridge);
      this._logger.info('Connected to bridge: %s', bridge.url);
      next();
    });
  }, (err) => {
    this._connectBridgesRunning = false;
    if (err) {
      return this._logger.error('Unable to connect to bridges');
    }
    this.emit('bridgesConnected');
  });
}

/**
 * Will connect to configured bridges to start receiving storage
 * contracts from them. If the contact is already at the bridge it will
 * update the contact details, otherwise it will add the contact to
 * and begin the benchmarking phase.
 */
FarmerInterface.prototype.connectBridges = function() {
  this._connectBridges();
  this._connectBridgesInterval = setInterval(
    this._connectBridges.bind(this),
    FarmerInterface.CONNECT_BRIDGE_INTERVAL
  );
};

/**
 * Mark if there is space available for accepting offers
 */
FarmerInterface.prototype.runSpaceCheck = function() {
  this._runSpaceCheck();
  this._spaceCheckInterval = setInterval(
    this._runSpaceCheck.bind(this),
    FarmerInterface.CHECK_SPACE_USED_INTERVAL
  );
}

FarmerInterface.prototype._runSpaceCheck = function() {

  this._logger.debug('running free space check');

  diskusage.check(this._options.storagePath, (err, info) => {
    if (err) {
      this._logger.warn('unable to check disk usage in interval, reason: %s',
                        err.message);
      return;
    }
    if (info.available <= FarmerInterface.FREE_SPACE_PADDING) {
      this.noSpaceLeft(true);
    } else {
      this.noSpaceLeft(false);
    }
  });
}

/**
 * This will change the state of the farmer so that it will stop receiving
 * messages to store data.
 */
FarmerInterface.prototype.noSpaceLeft = function(noSpace) {
  if (this.spaceAvailable === !noSpace) {
    return;
  }

  this._logger.debug('changing spaceAvailable to %s', !noSpace);
  this.spaceAvailable = !noSpace;
  let keys = this.bridges.keys();

  for (let i = 0; i < keys.length; i++) {
    // Set to false so that the contact will be updated
    this.bridges.get(keys[i]).connected = false;
  }
};

FarmerInterface.prototype._connectBridge = function(bridge, callback) {
  let headers = {};
  let body = {};
  let path = '/contacts/' + this.contact.nodeID;
  this.bridgeRequest(bridge.url, 'GET', path, headers, body, (err, contact) => {
    if (err && err.statusCode !== 404) {
      return callback(err);
    } else if (err && err.statusCode === 404) {
      return this._addBridgeContact(bridge, callback);
    }

    if (contact.address !== this.contact.address ||
        contact.port !== this.contact.port ||
        contact.spaceAvailable !== this.spaceAvailable ||
        contact.protocol !== this.contact.protocol) {
      this._updateBridgeContact(bridge, callback);
    } else {
      callback();
    }
  });
}

FarmerInterface.prototype._addBridgeContact = function(bridge, callback) {
  let target = null;
  let challenge = null;
  let nonce = null;

  async.series([
    (next) => {
      let headers = {};
      let body = {};
      let path = '/contacts/challenges';
      this.bridgeRequest(
        bridge.url, 'POST', path, headers, body, (err, data) => {
          if (err) {
            return callback(err);
          }
          target = data.target;
          challenge = data.challenge;
          next();
        }
      );
    },
    (next) => {
      this._completeChallenge(challenge, target, (err, _nonce) => {
        if (err) {
          return next(err);
        }
        nonce = _nonce;
        next();
      });
    },
    (next) => {
      let path = '/contacts';
      let headers = {
        'x-challenge': challenge,
        'x-challenge-nonce': nonce
      };
      let body = {
        address: this.contact.address,
        port: this.contact.port,
        spaceAvailable: true,
        protocol: this.contact.protocol
      }
      this.bridgeRequest(bridge.url, 'POST', path, headers, body, next);
    }
  ], callback);
}

FarmerInterface.prototype._updateBridgeContact = function(bridge, callback) {
  const headers = {};
  const body = {
    address: this.contact.address,
    port: this.contact.port,
    spaceAvailable: this.spaceAvailable,
    protocol: this.contact.protocol
  };

  this.bridgeRequest(bridge.url,
                     'PATCH',
                     '/contacts/' + this.contact.nodeID,
                     headers,
                     body,
                     callback)
}

FarmerInterface.prototype._completeChallenge = function(challenge,
                                                        target,
                                                        callback) {
  const powScript = path.resolve(__dirname, './pow.js');
  const args = [
    powScript,
    JSON.stringify({challenge: challenge, target: target})
  ];
  const options = {
    timeout: 900000
  };
  this.emit('bridgeChallenge');
  /* eslint no-unused-vars:0 */
  execFile(process.execPath, args, options, (err, stdout, stderr) => {
    if (err) {
      return callback(err);
    }
    let nonce = parseInt(stdout);
    if (!Number.isSafeInteger(nonce)) {
      return callback(null, new Error('Unexpected nonce'));
    }
    callback(null, nonce);
  });
}

/* eslint max-params:0 */
FarmerInterface.prototype._getSigHash = function(bridgeUrl,
                                                 method,
                                                 path,
                                                 timestamp,
                                                 rawbody) {
  const hasher = crypto.createHash('sha256');
  hasher.update(method);
  hasher.update(bridgeUrl + path);
  hasher.update(timestamp.toString());
  hasher.update(rawbody);
  return hasher.digest();
}

/* eslint max-params:0 */
// eslint-disable-next-line max-statements
FarmerInterface.prototype.bridgeRequest = function(bridgeUrl,
                                                   method,
                                                   path,
                                                   headers,
                                                   body,
                                                   callback) {

  const urlObj = url.parse(bridgeUrl);
  const timestamp = Date.now();
  const rawbody = JSON.stringify(body);
  const sighash = this._getSigHash(bridgeUrl, method, path, timestamp, rawbody);

  const privkey = Buffer.from(this.keyPair.getPrivateKeyPadded(), 'hex');
  const sigObj = secp256k1.sign(sighash, privkey);
  const sig = secp256k1.signatureExport(sigObj.signature).toString('hex');

  headers['x-node-timestamp'] = timestamp;
  headers['x-node-id'] = this.contact.nodeID;
  headers['x-node-signature'] = sig;
  headers['x-node-pubkey'] = this.keyPair.getPublicKey();
  headers['content-type'] = 'application/json';
  headers['content-length'] = Buffer.byteLength(rawbody);

  const options = {
    headers: headers,
    method: method,
    path: path,
    hostname: urlObj.hostname,
    port: parseInt(urlObj.port)
  };

  let proto = null;
  if (urlObj.protocol === 'https:') {
    proto = https;
  } else if (urlObj.protocol === 'http:') {
    proto = http;
  } else {
    return callback(new Error('Unsupported protocol'));
  }

  const req = proto.request(options, (res) => {

    let str = '';
    let json = null;

    if (res.statusCode >= 400) {
      let error = new Error('Bridge request failed (' + res.statusCode + ')');
      error.statusCode = res.statusCode;
      return callback(error);
    }

    res.setEncoding('utf8');

    res.on('data', (chunk) => {
      str += chunk.toString();
    });

    res.on('end', () => {
      try {
        json = JSON.parse(str);
      } catch (err) {
        callback(new Error('Unable to parse response'));
      }

      callback(null, json);
    });

  });

  req.on('error', callback);

  req.write(rawbody);
  req.end();
}

/**
 * Sends the given contract as an offer to the specified renter
 * @private
 * @param {Contract} contract - The contract to include in offer
 * @param {Contact} renter - The renter who originally published the contract
 */
FarmerInterface.prototype._sendOfferForContract = function(contract, contact) {
  var self = this;
  var message = new kad.Message({
    method: 'OFFER',
    params: {
      contract: contract.toObject(),
      contact: self.contact
    }
  });

  self._logger.debug('Sending offer for contract hash %s',
    contract.get('data_hash'));
  self.transport.send(contact, message, function(err, response) {
    if (err) {
      return self._logger.warn(err.message);
    }

    if (response.error || !response.result.contract) {
      return self._logger.warn(
        response.error ? response.error.message : 'Renter refused to sign'
      );
    }

    self._handleOfferRes(response, contract, contact);
  });
};

/**
 * Returns the payment address supplied or the derived one from keypair
 * @returns {String}
 */
FarmerInterface.prototype.getPaymentAddress = function() {
  return this._options.paymentAddress || this.keyPair.getAddress();
};

/**
 * Handles a received contract and negotiates storage
 * @private
 * @param {Contract} contract
 */
FarmerInterface.prototype._negotiateContract = function(contract, contact) {
  var self = this;

  contract.set('farmer_id', self.keyPair.getNodeID());
  contract.set('payment_destination', self.getPaymentAddress());
  contract.sign('farmer', self.keyPair.getPrivateKeyPadded());

  var item = new StorageItem({ hash: contract.get('data_hash') });
  var renterId = contract.get('renter_id');

  if (typeof renterId !== 'string') {
    return self._logger.warn('dropping invalid contract with no renter id');
  }

  item.addContract({ nodeID: renterId }, contract);
  item.addMetaData({ nodeID: renterId }, {});

  self.storageManager.save(item, function(err) {
    if (err) {
      return self._logger.error(err.message);
    }

    self._sendOfferForContract(contract, contact);
  });
};

/**
 * Checks if we should send an offer by checking the pending offers and running
 * the optional custom negotiator function
 * @private
 * @param {Contract} contract
 * @param {Function} callback
 */
FarmerInterface.prototype._shouldSendOffer = function(contract, callback) {
  var self = this;

  if (!this.spaceAvailable) {
    self._logger.debug('No space available to accept offer');
    return callback(false);
  }

  this._negotiator.call(this, contract, function(shouldNegotiate) {
    /* eslint max-statements: [2, 16] */
    self._logger.debug('negotiator returned: %s', shouldNegotiate);
    self.storageManager._storage.size(
      contract.get('data_hash'),
      function(err, usedSpace, contractDBSize) {
        if (err) {
          self._logger.error('Could not get usedSpace: %s',err.message);
          return callback(false);
        }

        var maxCapacity = self.storageManager._options.maxCapacity;
        var estimatedMaxBucketSize = Math.floor(
          (maxCapacity - contractDBSize) / kfs.constants.B
        );
        var freeSpace = estimatedMaxBucketSize - usedSpace;
        var enoughFreeSpace = contract.get('data_size') <= freeSpace;
        self._logger.debug(
          'max KFS bucket size %s, used %s, free %s, shard size %s',
          estimatedMaxBucketSize,
          usedSpace,
          freeSpace,
          contract.get('data_size'));
        self._logger.debug('we have enough free space: %s', enoughFreeSpace);

        callback(shouldNegotiate && enoughFreeSpace);
      }
    );
  });
};

/**
 * Handles an offer response from a renter
 * @private
 */
FarmerInterface.prototype._handleOfferRes = function(res, contract, renter) {
  var self = this;
  var final = null;

  try {
    final = Contract.fromObject(res.result.contract);
  } catch (err) {
    return self._logger.warn('renter responded with invalid contract');
  }

  if (!final.verify('renter', contract.get('renter_id'))) {
    return self._logger.warn('renter signature is invalid');
  }

  self.storageManager.load(contract.get('data_hash'), function(err, item) {
    if (err) {
      item = new StorageItem({ hash: contract.get('data_hash') });
    }

    item.addContract(renter, contract);
    item.addMetaData(renter, {});
    self.storageManager.save(item, utils.noop);
    self._logger.info('Offer accepted');
  });
};

/**
 * Subscribes to a contract identifier on the network
 * @private
 * @param {Array} opcodes
 */
FarmerInterface.prototype._listenForContracts = function(opcodes) {
  this.subscribe(opcodes, this._handleContractPublication.bind(this));
};

/**
 * Handles received contract publications
 * @private
 * @param {Object} contract - The raw contract object
 */
FarmerInterface.prototype._handleContractPublication = function(contract) {
  var self = this;
  var contractObj;
  var contact = contract.contact;

  this._logger.debug('received contract offer...');

  try {
    contractObj = Contract.fromObject(contract);
  } catch (err) {
    return; // If the contract is invalid just drop it
  }

  if (!contractObj.verify('renter', contractObj.get('renter_id'))) {
    return; // If the renter signature is invalid just drop it
  }

  this._shouldSendOffer(contractObj, function(shouldSendOffer) {
    if (!shouldSendOffer) {
      return self._logger.debug('not sending an offer for the contract');
    }

    self._negotiateContract(contractObj, contact);
  });
};

/**
 * Handles ALLOC messages
 * @param {Object} params
 * @param {Protocol~handleConsignCallback} callback
 */
FarmerInterface.prototype.handleAlloc = function(params, callback) {
  var self = this;
  var token = utils.generateToken();

  let contractObj = null;

  try {
    contractObj = Contract.fromObject(params.contract);
  } catch (err) {
    return callback(new Error('Invalid contract'));
  }

  if (!contractObj.verify('renter', contractObj.get('renter_id'))) {
    return callback(new Error('Invalid contract signature'));
  }

  self._logger.info('Droping alloc request to force a timeout from %s hash %s size %s',
                    params.contact.nodeID,
                    contractObj.get('data_hash'),
                    contractObj.get('data_size'));
  return;

  this._shouldSendOffer(contractObj, function(shouldSendOffer) {
    if (!shouldSendOffer) {
      // TODO give back a reason
      self._logger.debug('not sending an offer for the contract');
      return callback(new Error('Not accepting contracts'));
    }

    contractObj.set('farmer_id', self.keyPair.getNodeID());
    contractObj.set('payment_destination', self.getPaymentAddress());
    contractObj.sign('farmer', self.keyPair.getPrivateKeyPadded());

    var item = new StorageItem({ hash: contractObj.get('data_hash') });
    var renterId = contractObj.get('renter_id');

    if (typeof renterId !== 'string') {
      self._logger.warn('dropping invalid contract with no renter id');
      return callback(new Error('Invalid renter id'));
    }

    item.addContract({ nodeID: renterId }, contractObj);
    item.addMetaData({ nodeID: renterId }, {});

    self.storageManager.save(item, function(err) {
      if (err) {
        self._logger.error(err.message);
        return callback(new Error('Error saving contract'));
      }

      self.transport.shardServer.accept(
        token,
        contractObj.get('data_hash'),
        params.contact
      );

      if (self._contractCount < Number.MAX_SAFE_INTEGER) {
        self._contractCount++;
      } else {
        self._contractCount = 0;
      }

      self._logger.info('Sending alloc response hash %s size %s',
                        contractObj.get('data_hash'),
                        contractObj.get('data_size'));

      callback(null, { token: token, contract: contractObj.toObject() });

    });

  });
}

module.exports = FarmerInterface;
