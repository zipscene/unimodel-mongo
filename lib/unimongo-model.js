const SchemaModel = require('zs-unimodel').SchemaModel;
const objtools = require('zs-objtools');
const pasync = require('pasync');
const XError = require('xerror');
const UnimongoError = require('./unimongo-error');

class UnimongoModel extends SchemaModel {

	constructor(collectionName, schema, db, options = {}) {
		// Initialize superclass
		super(schema, options);

		// Set class variables
		this.db = db;
		this.dbPromise = db.dbPromise;
		this.collectionName = collectionName;

		// Translate options
		this.options = options;

		// Map from our option names to mongo collection options
		let collectionOptionMap = {
			w: 'w',
			writeConcern: 'w',
			wtimeout: 'wtimeout',
			writeConcernTimeout: 'wtimeout',
			j: 'j',
			journalWriteConcern: 'j',
			raw: 'raw',
			pkFactory: 'pkFactory',
			readPreference: 'readPreference',
			serializeFunctions: 'serializeFunctions',
			capped: 'capped',
			size: 'size',
			cappedSize: 'size',
			max: 'max',
			cappedDocuments: 'max',
			autoIndexId: 'autoIndexId'
		};
		this.collectionOptions = {};
		for (let opt in collectionOptionMap) {
			if (opt in options) {
				this.collectionOptions[collectionOptionMap[opt]] = options[opt];
			}
		}

		// Set up the promise for returning this collection
		this.collectionPromise = new Promise((resolve, reject) => {
			this.collectionPromiseResolve = resolve;
			this.collectionPromiseReject = reject;
		});

		// Start initializing the collection
		if (options.initialize !== false) {
			this.initCollection();
		}
	}

	_ensureExists(create = true) {
		return this.dbPromise
			.then((db) => {
				return new Promise((resolve, reject) => {
					db.collection(
						this.collectionName,
						objtools.merge({}, this.collectionOptions, { strict: true }),
						function(err, collection) {
							if (err) {
								reject(UnimongoError.fromMongoError(err));
							} else {
								resolve(collection);
							}
						}
					);
				});
			})
			.catch((err) {
				if (/does not exist/.test(err.message) && create) {
					// Collection doesn't yet exist.  Create it.
					return new Promise((resolve, reject) => {
						db.createCollection(
							this.collectionName,
							this.collectionOptions,
							function(err, collection) {
								if (err) {
									reject(err);
								} else {
									resolve(collection);
								}
							}
						);
					});
				} else {
					throw err;
				}
			});
	}

	_ensureIndices() {

	}

	initCollection() {
		return this._ensureExists()
			.then(() => this._ensureIndices())
			.then(this.collectionPromiseResolve)
			.catch((err) => {
				this.collectionPromiseReject(err);
				this.db.emit(
					'error',
					new XError(XError.DB_ERROR, 'Error initializing collection ' + this.collectionName, err)
				);
			})
			.catch(pasync.abort);
	}

	getName() {
		return this.collectionName;
	}

	find(query, options = {}) {

	}

}

module.exports = UnimongoModel;
