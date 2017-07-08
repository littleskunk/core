'use strict';

var constants = require('../constants');
var assert = require('assert');
var StorageAdapter = require('./adapter');
var StorageItem = require('./item');
var merge = require('merge');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var Logger = require('kad-logger-json');

/**
 * Interface for managing contracts, shards, and audits
 * @constructor
 * @license AGPL-3.0
 * @extends {EventEmitter}
 * @param {StorageAdapter} storage - Storage adapter to use
 * @param {Object} options
 * @param {Boolean} options.disableReaper - Don't perform periodic reaping of
 * stale contracts
 * @param {Object} [options.logger] - Logger to use for debugging
 * @param {Number} options.maxCapacity - Max number of bytes to allow in storage
 */
function StorageManager(storage, options) {
  if (!(this instanceof StorageManager)) {
    return new StorageManager(storage, options);
  }

  assert(storage instanceof StorageAdapter, 'Invalid storage adapter');

  this._options = merge(Object.create(StorageManager.DEFAULTS), options);
  this._storage = storage;
  this._logger = this._options.logger || new Logger(0);

  this._initShardReaper();
}

inherits(StorageManager, EventEmitter);

StorageManager.DEFAULTS = {
  disableReaper: false,
  maxCapacity: Infinity
};

/**
 * Loads the storage {@link Item} at the given key
 * @param {String} hash - Shard hash to load data for
 * @param {Function} callback - Called with error or {@link StorageItem}
 */
StorageManager.prototype.load = function(hash, callback) {
  assert(typeof hash === 'string', 'Invalid key supplied');
  assert(hash.length === 40, 'Key must be 160 bit hex string');
  assert(typeof callback === 'function', 'Callback function must be supplied');

  this._storage.get(hash, function(err, item) {
    if (err) {
      return callback(err);
    }

    if (!(item instanceof StorageItem)) {
      return callback(new Error('Storage adapter provided invalid result'));
    }

    callback(null, item);
  });
};

/**
 * Saves the storage {@link StorageItem} at the given key
 * @param {StorageItem} item - The {@link StorageItem} to store
 * @param {Function} callback - Called on complete
 */
StorageManager.prototype.save = function(item, callback) {
  var self = this;

  assert(item instanceof StorageItem, 'Invalid storage item supplied');
  assert(typeof callback === 'function', 'Callback function must be supplied');

  self._storage.get(item.hash, function(err, existingItem) {
    self._storage.put(
      self._merge(existingItem, item),
      function(err) {
        if (err) {
          return callback(err);
        }

        callback(null);
      }
    );
  });
};

/**
 * Merges two storage items together
 * @private
 */
StorageManager.prototype._merge = function(item1, item2) {
  return new StorageItem(
    merge.recursive(
      true,
      item1 ?
        ((item1 instanceof StorageItem) ?
          item1.toObject() :
          StorageItem(item1).toObject()) :
        {},
      item2 ?
        ((item2 instanceof StorageItem) ?
          item2.toObject() :
          StorageItem(item2).toObject()) :
        {}
    )
  );
};

/**
 * Opens the underlying storage adapter
 * @param {Function} callback - Called on complete
 */
StorageManager.prototype.open = function(callback) {
  this._storage._open(callback);
};

/**
 * Closes the underlying storage adapter
 * @param {Function} callback - Called on complete
 */
StorageManager.prototype.close = function(callback) {
  this._storage._close(callback);
};

/**
 * Enumerates all storage contracts and reaps stale data
 * @param {Function} callback - Called on complete
 */
StorageManager.prototype.clean = function(callback) {
  var self = this;
  var timestamp = Date.now();

  var rstream = self._storage.createReadStream();
  self._logger.warn('starting shard reaper, checking for expired contracts limit %s', limit);

  rstream.on('data', function(item) {
    rstream.pause();

    var total = Object.keys(item.contracts).length;
    var endedOrIncomplete = 0;

    for (var nodeID in item.contracts) {
      var contract = item.contracts[nodeID];
      var ended = contract.get('store_end') < timestamp;
      var incomplete = !contract.isComplete();

      if (ended || incomplete) {
        endedOrIncomplete++;
      }
    }

    if (total === endedOrIncomplete) {
      self._logger.info('destroying shard/contract for %s', item.hash);
      self._storage.del(item.hash, function(/* err */) {
        rstream.resume();
      });
    } else {
      rstream.resume();
    }
  });

  rstream.on('end', function() {
    callback();
  });
};

/**
 * Initialize the shard reaper to check for stale contracts and reap shards
 * @private
 */
StorageManager.prototype._initShardReaper = function() {
  if (this._options.disableReaper) {
    return false;
  }

  setTimeout(this.clean.bind(this, this._initShardReaper.bind(this)),
             constants.CLEAN_INTERVAL);
};

module.exports = StorageManager;
