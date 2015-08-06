const SchemaModel = require('zs-unimodel').SchemaModel;
const objtools = require('zs-objtools');
const pasync = require('pasync');
const XError = require('xerror');
const UnimongoError = require('./unimongo-error');
const _ = require('lodash');

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

		// Initialize the list of indices on the collection
		// This contains elements in this format:
		// `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
		this._indices = this._getSchemaIndices();

		// Latch to prevent double-initialization
		this._startedInitializing = false;

		// Start initializing the collection
		if (options.initialize !== false) {
			setImmediate(() => this.initCollection());
		}
	}

	/**
	 * Add an index to this collection.
	 *
	 * @method index
	 * @param {Object} spec - A MongoDb index spec, like: `{ foo: 1, bar: '2dsphere' }`
	 * @return {UnimongoModel} - This model, for chaining
	 */
	index(spec, options = {}) {
		// Convert the index type values
		spec = _.mapValues(spec, (indexType) => UnimongoModel._convertIndexType(indexType));
		this._indices.push({
			spec,
			options
		});
		return this;
	}

	/**
	 * Converts a user-specified index type into a mongo-compatible index type.
	 *
	 * @method _convertIndexType
	 * @private
	 * @static
	 * @param {Mixed} indexType - User-supplied index type
	 * @param {Object} [subschema] - Subschema data for datatype (used for type hints)
	 * @return {Mixed} - Mongo index type
	 */
	static _convertIndexType(indexType, subschema) {
		const indexTypeMap = {
			'1': 1,
			'-1': -1,
			forward: 1,
			reverse: -1,
			'2dsphere': '2dsphere',
			geo: '2dsphere',
			'2d': '2d',
			geoHaystack: 'geoHaystack',
			text: 'text',
			hashed: 'hashed'
		};

		if (indexType === true) {
			// Try to autodetermine the index type
			if (subschema && (subschema.type === 'geojson' || subschema.type === 'geopoint')) {
				return '2dsphere';
			} else {
				return 1;
			}
		} else if (indexTypeMap[''+indexType]) {
			return indexTypeMap[''+indexType];
		} else {
			throw new XError(XError.INVALID_ARGUMENT, 'Invalid index type: ' + indexType);
		}
	}

	/**
	 * Returns a list of indices that should be on this collection based on indices defined
	 * in the schema.
	 * Each index is represented as an object with two properties:
	 * `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
	 *
	 * @method _getSchemaIndices
	 * @private
	 * @return {Object[]} - Array of indices as noted above
	 */
	_getSchemaIndices() {
		let allIndices = [];

		// Crawl the schema to find any indexed single fields
		this.schema.traverseSchema({
			onSubschema(subschema, path, subschemaType) {
				if (subschema.index || subschema.unique) {
					let indexEntry = {
						spec: {
							[path]: UnimongoModel._convertIndexType(subschema.index || true, subschema)
						},
						options: {}
					};
					if (subschema.sparse) {
						indexEntry.options.sparse = true;
					}
					if (subschema.unique) {
						indexEntry.options.unique = true;
					}
					allIndices.push(indexEntry);
				}
			}
		}, {
			includePathArrays: false
		});

		return allIndices;
	}

	/**
	 * Returns all indices on this model.
	 *
	 * @method getIndices
	 * @return {Object[]} - Each entry is in the form:
	 *   `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
	 */
	getIndices() {
		return this._indices;
	}

	/**
	 * Creates a collection with the specified options if the collection does not exist.
	 *
	 * @method _ensureExists
	 * @private
	 * @param {Boolean} [create=true] - Whether or not to create the collection if it doesn't
	 *   exist.  This is intended to be used when this function calls itself recursively to
	 *   prevent infinite loops in the case of errors.
	 * @return {Promise} - Resolves with the mongo driver Collection object.
	 */
	_ensureExists(create = true) {
		let db;
		return this.dbPromise
			.then((_db) => {
				db = _db;
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
			.catch((err) => {
				if (/does not exist/.test(err.message) && create) {
					// Collection doesn't yet exist.  Create it.
					return new Promise((resolve, reject) => {
						db.createCollection(
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
				} else {
					throw err;
				}
			})
			.catch((err) => {
				if (/already exists/.test(err.message) && create) {
					return this._ensureExists(false);
				} else {
					throw err;
				}
			});
	}

	_ensureIndices(collection) {
		return collection;
	}

	initCollection() {
		if (this._startedInitializing) {
			return this.collectionPromise;
		}
		this._startedInitializing = true;

		return this._ensureExists()
			.then((collection) => this._ensureIndices(collection))
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
