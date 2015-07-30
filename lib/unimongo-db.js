const MongoClient = require('mongodb').MongoClient;
const objtools = require('zs-objtools');
const EventEmitter = require('events').EventEmitter;
const pasync = require('pasync');
const _ = require('lodash');
const UnimongoError = require('./unimongo-error');

/**
 * Class representing a connection to a MongoDB database.
 *
 * To use this class, instantiate it with `new UnimongoDb()` and then call `connect()`.
 * The class contains a promise on `unimongoDb.dbPromise` which resolves/rejects when the
 * database connection succeeds/fails.  This promise exists before the connection has started,
 * so it's safe to use it before connecting.
 *
 * This class also emits the following events:
 *
 * - connect(db) - Emitted when the database connects or reconnects.
 * - disconnect(reason) - Emitted when the database disconnects for some reason.
 * - error(err) - Emitted when some asynchronous error occurs.
 *
 * @class UnimongoDb
 * @constructor
 * @param {Object} [options] - Any options that can be passed to `connect()` .
 */
class UnimongoDb extends EventEmitter {

	constructor(options = {}) {
		super();
		// This promise is initialized on construction, but the connection isn't actually started
		// until _connectInternal() is called.  This allows the UnimongoDb object to be used to
		// construct UnimongoModel objects before actually connecting to mongo.
		this.dbPromise = new Promise((resolve, reject) => {
			this.dbPromiseResolve = resolve;
			this.dbPromiseReject = reject;
		});
		this.options = options;
	}

	/**
	 * Sets up a connection to a mongodb server.
	 *
	 * @method connect
	 * @param {String} uri - Mongo connection string.  See http://docs.mongodb.org/manual/reference/connection-string/
	 * @param {Object} [options] - Any option that can be passed into `MongoClient.connect()` .
	 *   Additionally, options that are passed in on the root options object are separated into
	 *   the separate suboptions on the `db`, `server`, `replSet`, and `mongos` objects.
	 * @return {Promise} - Resolves when the connection is complete.  Note that this promise will not
	 *   reject on error.  Because errors can occur at any time, not just during the connection, this
	 *   object will emit `error` events.  To detect and handle errors, listen to the `error` event
	 *   instead of waiting for this promise to reject.
	 */
	connect(uri, options = {}) {
		// Merge options
		options = _.merge({}, this.options || {}, options);
		if (!uri && options.uri) {
			uri = options.uri;
		}
		if (!uri) {
			throw new Error('No URI supplied to UnimongoDb');
		}
		// Map from our option names to Mongo Db class option names
		let dbOptionNameMap = {
			authSource: 'authSource',
			w: 'w',
			writeConcern: 'w',
			wtimeout: 'wtimeout',
			writeConcernTimeout: 'wtimeout',
			j: 'j',
			journalWriteConcern: 'j',
			native_parser: 'native_parser', // eslint-disable-line camelcase
			nativeParser: 'native_parser',
			forceServerObjectId: 'forceServerObjectId',
			serializeFunctions: 'serializeFunctions',
			raw: 'raw',
			promoteLongs: 'promoteLongs',
			bufferMaxEntries: 'bufferMaxEntries',
			numberOfRetries: 'numberOfRetries',
			reconnectTries: 'numberOfRetries',
			retryMiliSeconds: 'retryMiliSeconds',
			retryMilliseconds: 'retryMiliSeconds',
			reconnectInterval: 'retryMiliSeconds',
			readPreference: 'readPreference',
			pkFactory: 'pkFactory'
		};
		// Same for Server class
		let serverOptionNameMap = {
			poolSize: 'poolSize',
			ssl: 'ssl',
			sslValidate: 'sslValidate',
			sslCA: 'sslCA',
			sslCert: 'sslCert',
			sslKey: 'sslKey',
			sslPass: 'sslPass',
			socketOptions: 'socketOptions',
			numberOfRetries: 'reconnectTries',
			reconnectTries: 'reconnectTries',
			retryMiliSeconds: 'reconnectTries',
			retryMilliseconds: 'reconnectTries',
			reconnectInterval: 'retryMiliSeconds'
		};
		// Same for ReplSet class
		let replSetOptionNameMap = {
			ha: 'ha',
			highAvailabilityMonitoring: 'ha',
			replicaSet: 'replicaSet',
			secondaryAcceptableLatencyMS: 'secondaryAcceptableLatencyMS',
			connectWithNoPrimary: 'connectWithNoPrimary',
			poolSize: 'poolSize',
			ssl: 'ssl',
			sslValidate: 'sslValidate',
			sslCA: 'sslCA',
			sslCert: 'sslCert',
			sslKey: 'sslKey',
			sslPass: 'sslPass',
			socketOptions: 'socketOptions'
		};
		// Same for Mongos class
		let mongosOptionNameMap = {
			ha: 'ha',
			highAvailabilityMonitoring: 'ha',
			haInterval: 'haInterval',
			poolSize: 'poolSize',
			ssl: 'ssl',
			sslValidate: 'sslValidate',
			sslCA: 'sslCA',
			sslCert: 'sslCert',
			sslKey: 'sslKey',
			sslPass: 'sslPass',
			socketOptions: 'socketOptions'
		};

		// Alias options and split into the 4 groups
		let optionGroupMaps = {
			db: dbOptionNameMap,
			server: serverOptionNameMap,
			replSet: replSetOptionNameMap,
			mongos: mongosOptionNameMap
		};
		for (let optionGroupName in optionGroupMaps) {
			if (!options[optionGroupName]) {
				options[optionGroupName] = {};
			}
			let optionNameMap = optionGroupMaps[optionGroupName];
			for (let optionName in optionNameMap) {
				if (options[optionGroupName][optionName] === undefined && options[optionName] !== undefined) {
					options[optionGroupName][optionName] = options[optionName];
				}
			}
		}

		// Set mandatory options
		options.promiseLibrary = Promise;

		// Start connecting
		MongoClient.connect(uri, options)
			.catch((err) => {
				throw UnimongoError.fromMongoError(err);
			})
			.then((db) => {
				db.on('error', (err) => this.emit('error', err) );
				db.on('timeout', () => this.emit('disconnect', 'Timed out') );
				db.on('close', () => this.emit('disconnect', 'Connection closed') );
				db.on('reconnect', () => this.emit('connect', db) );
				this.emit('connect', db);
				this.dbPromiseResolve(db);
				this.db = db;
			}, (err) => {
				this.dbPromiseReject(err);
				this.emit('error', err);
			})
			.catch(pasync.abort);

		return this.dbPromise;
	}

	/**
	 * Closes this database connection.
	 *
	 * @method close
	 * @return {Promise} - Resolves when closed.
	 */
	close() {
		if (this.db) {
			return new Promise((resolve, reject) => {
				this.db.close((err) => {
					if (err) {
						reject(UnimongoError.fromMongoError(err));
					} else {
						resolve();
					}
				});
				this.db = null;
				this.dbPromise = new Promise((resolve, reject) => {
					this.dbPromiseResolve = resolve;
					this.dbPromiseReject = reject;
				});
			});
		} else {
			return Promise.resolve();
		}
	}

	/**
	 * Drops the whole database.
	 *
	 * @method dropDatabase
	 * @return {Promise}
	 */
	dropDatabase() {
		return this.dbPromise.then((db) => {
			return new Promise((resolve, reject) => {
				db.dropDatabase((err) => {
					if (err) {
						reject(err);
					} else {
						resolve();
					}
				});
			});
		});
	}

}

module.exports = UnimongoDb;
