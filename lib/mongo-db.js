// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const _ = require('lodash');
const mongodb = require('mongodb');
const EventEmitter = require('events').EventEmitter;
const pasync = require('pasync');
const XError = require('xerror');
const MongoError = require('./mongo-error');
const MongoModel = require('./mongo-model');
const opUtils = require('./utils/ops');

/**
 * Class representing a connection to a MongoDB database.
 *
 * To use this class, instantiate it with `new MongoDb()` and then call `connect()`.
 * The class contains a promise on `mongoDb.dbPromise` which resolves/rejects when the
 * database connection succeeds/fails.  This promise exists before the connection has started,
 * so it's safe to use it before connecting.
 *
 * This class also emits the following events:
 *
 * - connect(db) - Emitted when the database connects or reconnects.
 * - disconnect(reason) - Emitted when the database disconnects for some reason.
 * - error(err) - Emitted when some asynchronous error occurs.
 *
 * @class MongoDb
 * @constructor
 * @param {Object} [options] - Any options that can be passed to `connect()` .
 */
class MongoDb extends EventEmitter {

	constructor(options = {}) {
		super();
		// This promise is initialized on construction, but the connection isn't actually started
		// until _connectInternal() is called.  This allows the MongoDb object to be used to
		// construct MongoModel objects before actually connecting to mongo.
		this.dbPromise = new Promise((resolve, reject) => {
			this.dbPromiseResolve = resolve;
			this.dbPromiseReject = reject;
		});
		this.options = options;
		this.modelRegistry = {};  // Mapping object for models registered to this db
	}

	/**
	 * Create a model, i.e. an underlying Mongo collection, to be used with this MongoDB.
	 *
	 * @method createModel
	 * @param {String} modelName - The name of the model.
	 * @param {Mixed} schema - The schema for the new model.
	 * @param {Mixed} [options] - Options to be passed down to the MongoDB collection.
	 * @return {MongoModel}
	 */
	createModel(modelName, schema = {}, options = {}) {
		return new MongoModel(modelName, schema, this, options);
	}

	/**
	 * A dual purpose method for dealing with models. If called with a fully configured MongoModel object, the
	 * model will be registered to this db. If called with the string name of a perviously registered model, it will
	 * return that model.
	 *
	 * @method model
	 * @param {MongoModel|String} model
	 * @return {MongoModel}
	 */
	model(model) {
		if (_.isObject(model)) {
			this.modelRegistry[model.getName()] = model;
		} else if (_.isString(model)) {
			let name = model;
			model = this.modelRegistry[name];
			if (!model) throw new XError(XError.INTERNAL_ERROR, `Model not found: ${name}`);
			return model;
		} else {
			throw new XError(XError.INVALID_ARGUMENT, `argument must either be a string or a model instance`);
		}
	}


	/**
	 * Sets up a connection to a mongodb server.
	 *
	 * @method connect
	 * @param {String} uri - Mongo connection string.  See http://docs.mongodb.org/manual/reference/connection-string/
	 * @param {Object} [options] - Any option that can be passed into `mongodb.MongoClient.connect()` .
	 *   Additionally, options that are passed in on the root options object are separated into
	 *   the separate suboptions on the `db`, `server`, `replSet`, and `mongos` objects.
	 *   @param {Boolean} [options.autoCreateIndex] - create indexes automatically if set to true. Default to true.
	 *   @param {Boolean} options.backgroundIndex - create indexes in background mode if set to true.
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
			throw new Error('No URI supplied to MongoDb');
		}

		if (options.autoCreateIndex !== false) this.options.autoCreateIndex = true;
		else this.options.autoCreateIndex = false;
		this.options.backgroundIndex = options.backgroundIndex;

		let mongoOptionMap = {
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
			pkFactory: 'pkFactory',
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
			reconnectInterval: 'retryMiliSeconds',
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
			socketOptions: 'socketOptions',
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

		let mongoConnectOptions = {};
		for (let name in mongoOptionMap) {
			let value = options[name];
			let optionName = mongoOptionMap[name];
			if (value !== undefined && mongoConnectOptions[optionName] === undefined) {
				mongoConnectOptions[optionName] = value;
			}
		}

		// Set mandatory options
		mongoConnectOptions.promiseLibrary = Promise;
		mongoConnectOptions.useNewUrlParser = true;

		// Start connecting
		return pasync.retry({ times: 10, interval: 100 }, () => {
			return mongodb.MongoClient.connect(uri, mongoConnectOptions);
		})
			.catch((err) => {
				throw MongoError.fromMongoError(err);
			})
			.then((db) => {
				db.on('error', (err) => this.emit('error', err) );
				db.on('timeout', () => this.emit('disconnect', 'Timed out') );
				db.on('close', () => this.emit('disconnect', 'Connection closed') );
				db.on('reconnect', () => this.emit('connect', db) );
				this.emit('connect', db);
				this.dbPromiseResolve(db);
				this.db = db;

				const dbAdmin = db.admin();
				return Promise.resolve()
					// Get  and store server info
					.then(() => dbAdmin.serverInfo())
					.then((serverInfo) => (this.serverInfo = serverInfo))
					// Try to get number of replicas,
					// ignoring errors since they are expected in non-replicated environments
					.then(() => dbAdmin.replSetGetStatus())
					.then((status) => {
						let replicas = status.members.filter((member) => member.stateStr === 'SECONDARY');
						this.numReplicas = replicas.length;
					}, () => {
						this.numReplicas = 0;
					});
			}, (err) => {
				this.dbPromiseReject(err);
				this.emit('error', err);
			})
			.catch(pasync.abort)
			.then(() => this.dbPromise);
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
						reject(MongoError.fromMongoError(err));
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

	/**
	 * Sends a kill command to any in-progress operations with the provided
	 * operation id.
	 *
	 * @method killOperation
	 * @param {String} operationId
	 * @return {Promise} - Resolves when the operation is complete.
	 */
	killOperation(operationId) {
		return this._getOpIdsWithComment(operationId)
			.then((opIds) => this._killOps(opIds));
	}

	/**
	 * Searches through the results of a currentOp command to find the
	 * opIds of any in-progress operations with a top-level $comment field
	 * matching the provided string.
	 *
	 * @method _getOpIdsWithComment()
	 * @private
	 * @param {String} comment
	 * @return {Promise{Array}} - Resolves with an array of opIds.
	 */
	_getOpIdsWithComment(comment) {
		return this.db.admin().command({ currentOp: 1 })
			.then((currentOpDoc) => opUtils.getOpIdsWithComment(
				currentOpDoc,
				comment
			));
	}

	/**
	 * Sends a kill command to operations with the provided op ids.
	 *
	 * @method _killOps
	 * @private
	 * @param {Array{String}} opIds
	 * @return {Promise} - Resolves when the operation is complete.
	 */
	_killOps(opIds) {
		let admindb = this.db.admin();
		return pasync.each(opIds, (opId) => admindb.command({
			killOp: 1,
			op: opId
		}));
	}

}

module.exports = MongoDb;
