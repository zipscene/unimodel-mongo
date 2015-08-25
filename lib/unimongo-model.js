const SchemaModel = require('zs-unimodel').SchemaModel;
const objtools = require('zs-objtools');
const pasync = require('pasync');
const XError = require('xerror');
const UnimongoError = require('./unimongo-error');
const UnimongoDocument = require('./unimongo-document');
const commonQuery = require('zs-common-query');
const CursorResultStream = require('./cursor-result-stream');
const _ = require('lodash');

// Mapping from options we accept to the corresponding mongo option names
const mongoOptionMap = {
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

/**
 * MongoDB model class for Unimodel.
 *
 * @class UnimongoModel
 * @constructor
 */
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
		this.collectionOptions = UnimongoModel._transformMongoOptions(options);

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

	static _transformMongoOptions(options) {
		let res = {};
		for (let opt in options) {
			if (opt in mongoOptionMap) {
				res[mongoOptionMap[opt]] = options[opt];
			}
		}
		return res;
	}

	/**
	 * Add an index to this collection.
	 *
	 * @method index
	 * @param {Object} spec - A MongoDb index spec, like: `{ foo: 1, bar: '2dsphere' }`
	 * @return {UnimongoModel} - This model, for chaining
	 */
	index(spec, options = {}) {
		if (this._startedInitializing) {
			throw new XError(XError.INTERNAL_ERROR, 'Cannot add new indexes after initializing');
		}
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

	getKeys() {
		// Fetch the keys as follows:
		// - Allow a 'keys' option to be passed into the constructor, setting the array of keys.
		//   If this is passed in, return that.
		// - If that is not passed in, traverse the schema to find schema elements tagged with the
		//   { key: true } flag.  Return an array of field names, in traversal order, with this flag.
		//   This allows shorthand specification of keys.  Note that using this means that key fields
		//   in the schema must be declared in the order from most specific from least specific.
		// - If neither of those is passed in, find the largest index declared (greatest number of
		//   included fields) where the only index types are '1' and '-1' .  Treat this index as the
		//   set of keys.  Convert the index spec to an array of fields, and reverse the order.  Then
		//   return it.
		// - If no fields are indexed, throw an exception that no keys are declared.
		// - In any case, cache the computed array of keys on this object so the whole sequence above
		//   doesn't have to be checked each time.
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

	/**
	 * Internal function to create all indices on a collection.
	 *
	 * @method _ensureIndices
	 * @private
	 * @param {Collection} collection - The Mongo native driver collection
	 * @return {Promise} - Resolve with collection or rejects with XError
	 */
	_ensureIndices(collection) {
		return pasync.eachSeries(this.getIndices(), (indexInfo) => {
			return new Promise((resolve, reject) => {
				collection.createIndex(indexInfo.spec, indexInfo.options, (err) => {
					if (err) {
						reject(UnimongoError.fromMongoError(err));
					} else {
						resolve();
					}
				});
			});
		}).then(() => collection);
	}

	/**
	 * Initializes the Mongo collection.  This normally happens automatically on construction
	 * unless the `initialize` option was set to false.
	 *
	 * @method initCollection
	 * @return {Promise} - Returns this.collection
	 */
	initCollection() {
		if (this._startedInitializing) {
			return this.collectionPromise;
		}
		this._startedInitializing = true;

		this._ensureExists()
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

		return this.collectionPromise;
	}

	getName() {
		return this.collectionName;
	}

	create(data = {}) {
		return new UnimongoDocument(this, data);
	}

	/**
	 * Create a UnimongoDocument that represents an existing document in the database.  The data
	 * block given must include an _id field .
	 *
	 * @method _createExisting
	 * @private
	 * @param {Object} data
	 * @return {UnimongoDocument}
	 */
	_createExisting(data) {
		return new UnimongoDocument(this, data, true);
	}

	/**
	 * Executes a find using options instead of the Mongo driver's chaining.
	 *
	 * @method _findWithOptions
	 * @static
	 * @param {Collection} collection - The mongo native driver collection
	 * @param {Object} query - The query data
	 * @param {Object} options
	 * @return {Cursor} - Mongo native driver cursor
	 */
	static _findWithOptions(collection, query, options) {
		let cursor = collection.find(query);
		if ('skip' in options) {
			cursor = cursor.skip(options.skip);
		}
		if ('limit' in options) {
			cursor = cursor.limit(options.limit);
		}
		if ('fields' in options) {
			let projection = {};
			for (let field of options.fields) {
				projection[field] = true;
			}
			projection._id = true;
			cursor = cursor.project(projection);
		}
		if ('sort' in options) {
			let sortSpec = {};
			for (let field of options.sort) {
				if (field[0] === '-') {
					sortSpec[field[0].slice(1)] = -1;
				} else {
					sortSpec[field[0]] = 1;
				}
			}
			cursor = cursor.sort(sortSpec);
		}
		return cursor;
	}

	/**
	 * Find records in database
	 *
	 * @method find
	 * @param {commonQuery.Query} query - Query for records to find
	 * @param {Object} [options={}] - Mongo options
	 * @return {Array{UnimongoDocument}} - List of result documents
	 * @since v0.0.1
	 */
	find(query, options = {}) {
		// Transform the mongo options
		let mongoOptions = UnimongoModel._transformMongoOptions(options);

		// Transform the query according to the schema
		if (!_.isFunction(query.normalize)) query = commonQuery.createQuery(query);

		query.normalize({ schema: this.schema });

		return this.collectionPromise
			.then((collection) => collection.find(query.getData(), mongoOptions).toArray())
			.then((results) => {
				return _.map(results, (data) => this._createExisting(data));
			});
	}

	/**
	 * Find records in database
	 *
	 * @method findStream
	 * @param {commonQuery.Query} query - Query for records to find
	 * @param {Object} [options={}] - Mongo options
	 * @return {CursorResultStream} - List of result documents
	 * @since v0.0.1
	 */
	findStream(query, options = {}) {
		// Transform the query according to the schema
		if (!_.isFunction(query.normalize)) {
			query = commonQuery.createQuery(query);
		}
		query.normalize({ schema: this.schema });

		// Run the query, streaming the results
		let stream = new CursorResultStream(this);
		this.collectionPromise
			.then((collection) => {
				let cursor = UnimongoModel._findWithOptions(collection, query.getData(), options);
				stream.setCursor(cursor);
			}, (err) => {
				stream.emit('error', err);
			})
			.catch(pasync.abort);
		return stream;
	}

	/**
	 * Insert record into the database
	 *
	 * @method insertMulti
	 * @param {Object} data - Data to insert
	 * @param {Object} [options={}] - Mongo options
	 * @return {UnimongoDocument} - Result document
	 * @since v0.0.1
	 */
	insert(data, options = {}) {
		return this.insertMulti([ data ], options)
			.then((results) => results[0]);
	}

	/**
	 * Insert records into the database
	 *
	 * @method insertMulti
	 * @param {Array{Object}} datas - Array of data to insert
	 * @param {Object} [options={}] - Mongo options
	 * @return {Array{UnimongoDocument}} - List of result documents
	 * @since v0.0.1
	 */
	insertMulti(datas, options = {}) {
		// Transform the mongo options
		let mongoOptions = UnimongoModel._transformMongoOptions(options);

		// Normalize the data according to the schema
		for (let data of datas) {
			this.schema.normalize(data, options);
		}

		// Insert the documents
		return this.collectionPromise
			.then((collection) => {
				return collection.insertMany(datas, mongoOptions);
			})
			.then((result) => {
				if (
					!result ||
					!result.result ||
					!result.result.ok ||
					result.result.n !== datas.length ||
					!result.ops ||
					result.ops.length !== datas.length
				) {
					throw new UnimongoError(XError.DB_ERROR, 'Unexpected insert result', { result: result.result });
				}
				return _.map(result.ops, (data) => this._createExisting(data));
			}, (err) => {
				throw UnimongoError.fromMongoError(err);
			});
	}

	/**
	 * Count records in database
	 *
	 * @method count
	 * @param {commonQuery.Query} query - Query for record(s) to remove
	 * @param {Object} [options={}] - Mongo options
	 * @return {Number} - The number of matched records
	 * @since v0.0.1
	 */
	count(query, options = {}) {
		// Transform the mongo options
		let mongoOptions = UnimongoModel._transformMongoOptions(options);

		// Transform the query according to the schema
		if (!_.isFunction(query.normalize)) query = commonQuery.createQuery(query);

		query.normalize({ schema: this.schema });

		return this.collectionPromise
			.then((collection) => collection.count(query.getData(), mongoOptions));
	}

	aggregateMulti(query, aggregates, options = {}) {

	}

	/**
	 * Remove records from database
	 *
	 * @method remove
	 * @param {commonQuery.Query} query - Query for record(s) to remove
	 * @param {Object} [options={}] - Mongo options
	 * @return {Object} - The response from the mongo command
	 * @since v0.0.1
	 */
	remove(query, options = {}) {
		// Transform the mongo options
		let mongoOptions = UnimongoModel._transformMongoOptions(options);

		// Transform the query according to the schema
		if (!_.isFunction(query.normalize)) query = commonQuery.createQuery(query);

		query.normalize({ schema: this.schema });

		return this.collectionPromise
			.then((collection) => collection.remove(query.getData(), mongoOptions));
	}

	/**
	 * Update records in database
	 *
	 * @method update
	 * @param {commonQuery.Query} query - Query for record(s) to remove
	 * @param {commonQuery.Update} update - Update query
	 * @param {Object} [options={}] - Mongo options
	 * @return {Array{UnimongoDocument}} - List of result documents
	 * @since v0.0.1
	 */
	update(query, update, options = {}) {

	}

}

module.exports = UnimongoModel;
