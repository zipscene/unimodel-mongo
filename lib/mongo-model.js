const _ = require('lodash');
const pasync = require('pasync');
const XError = require('xerror');
const objtools = require('zs-objtools');
const SchemaModel = require('zs-unimodel').SchemaModel;
const MongoError = require('./mongo-error');
const MongoDocument = require('./mongo-document');
const aggregateUtils = require('./utils/aggregates');
const CursorResultStream = require('./cursor-result-stream');
const bson = require('bson');
const BSON = new bson.BSONPure.BSON();

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
	autoIndexId: 'autoIndexId',
	upsert: 'upsert'
};

/**
 * MongoDB model class for Unimodel.
 *
 * @class MongoModel
 * @constructor
 * @param {String} modelName
 * @param {Object} schema
 * @param {MongoDb} db
 * @param {Object} [options] - Additional options
 *   @param {Array{String}} options.keys - List of keys for the model
 *   @param {Boolean} options.allowSavingPartials - Whether to allow saving partial documents
 * @since v0.0.1
 */
class MongoModel extends SchemaModel {

	constructor(modelName, schema, db, options = {}) {
		// Initialize superclass
		super(schema, options);

		// Set class variables
		this.db = db;
		this.dbPromise = db.dbPromise;
		this.modelName = modelName;

		// Translate options
		this.options = options;
		if (typeof this.options.allowSavingPartials === 'undefined') this.options.allowSavingPartials = true;

		this.keys = this.options.keys;

		// Map from our option names to mongo collection options
		this.collectionOptions = MongoModel._transformMongoOptions(options, this.db);

		// Set up the promise for returning this collection
		this.collectionPromise = new Promise((resolve, reject) => {
			this.collectionPromiseResolve = resolve;
			this.collectionPromiseReject = reject;
		});

		// Initialize the list of indices on the collection
		// This contains elements in this format:
		// `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
		this._indices = this._getSchemaIndices();
		// Precalculate the indexed map fields
		this._indexedMapFields = [];
		this._addIndexedMapFields(this._indices);

		// Latch to prevent double-initialization
		this._startedInitializing = false;

		// Start initializing the collection
		if (this.options.initialize !== false) {
			setImmediate(() => this.initCollection());
		}
	}

	/**
	 * Transform our options into those accepted by mongo.
	 *
	 * @method _transformMongoOptions
	 * @static
	 * @param {Object} [options={}]
	 *   @param {String} options.readPreference - How to route read operations to members of a replica set.
	 *     In addition to the [normal MongoDB values](http://docs.mongodb.org/manual/reference/read-preference/),
	 *     may also be 'roundRobin', which cycles reads between primary and secondary members in the replica set.
	 * @param {MongoDb} [db] - Database instance to use for certain property checks such as `readPreference`
	 * @since v0.1.0
	 */
	static _transformMongoOptions(options, db) {
		let res = {};
		if (!options) options = {};

		for (let option in options) {
			if (option in mongoOptionMap) {
				// If a readPreference of 'roundRobin' is set, randomly choose from among the instances
				if (option === 'readPreference' && options[option] === 'roundRobin' && db) {
					let usePrimary = Math.random() * (db.numReplicas + 1) < 1;
					options[option] = (usePrimary) ? 'primaryPreferred' : 'secondaryPreferred';
				}

				res[mongoOptionMap[option]] = options[option];
			}
		}

		return res;
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
	 * @since v0.0.1
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
	 * Executes a find using options instead of the Mongo driver's chaining.
	 *
	 * @method _findWithOptions
	 * @static
	 * @param {mongodb.Collection} collection - The mongo native driver collection
	 * @param {Object} query - The query data
	 * @param {Object} options
	 * @return {mongodb.Cursor} - Mongo native driver cursor
	 * @since v0.0.1
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
					sortSpec[field.slice(1)] = -1;
				} else {
					sortSpec[field] = 1;
				}
			}
			cursor = cursor.sort(sortSpec);
		}

		return cursor;
	}

	/**
	 * Add an index to this collection.
	 *
	 * @method index
	 * @chainable
	 * @param {Object} spec - A MongoDb index spec, like: `{ foo: 1, bar: '2dsphere' }`
	 * @return {MongoModel} - This model, for chaining
	 * @since v0.0.1
	 */
	index(spec, options = {}) {
		if (this._startedInitializing) {
			throw new XError(XError.INTERNAL_ERROR, 'Cannot add new indexes after initializing');
		}
		// Convert the index type values
		let entry = {
			spec: {},
			options
		};
		let mapSets = {};
		// Convert all paths inside the spec
		for (let path in spec) {
			let { specPath, specIndex } = this._convertIndexSpec(path, spec[path]);
			if (!/^_mapidx_/.test(specPath)) {
				// This is a normal path don't do anything special
				entry.spec[specPath] = specIndex;
				continue;
			}
			// Cut off the '_mapidx_'
			let fullPath = specPath.substring(8);
			// Get array of fields (this should be length 2)
			let fullPathArray = fullPath.split('^');
			// The second field should be the field
			let field = fullPathArray[1];
			// The first field should be the map path
			let mapPath = fullPathArray[0];
			// Add the field to the map set
			if (!mapSets[mapPath]) { mapSets[mapPath] = []; }
			mapSets[mapPath].push(field);
		}

		// Handle any map indices
		if (!_.isEmpty(mapSets)) {
			if (_.size(mapSets) > 1) {
				// This will cause an NxN index
				throw new MongoError(MongoError.DB_ERROR, 'Cannot index accross maps multiple maps.');
			}
			// The the one mapSets key
			let mapPath = _.keys(mapSets)[0];
			// Concatenate all mapPath/field combinations
			let specPath = `_mapidx_${mapPath}`;
			for (let field of mapSets[mapPath].sort()) {
				specPath += `^${field}`;
			}
			entry.spec[specPath] = 1;
			this._addIndexedMapFields(entry);
		}

		this._indices.push(entry);
		return this;
	}

	/**
	 * Try to find indexed map fields in the given indices.
	 *
	 * @method _addIndexedMapFields
	 * @private
	 * @param {Object|Object[]} indices - The index, or indices to try adding indexed map fields from.
	 * @since v0.5.0
	 */
	_addIndexedMapFields(indices) {
		if (!_.isArray(indices)) { indices = [ indices ]; }
		for (let index of indices) {
			for (let field in index.spec) {
				if (/^_mapidx_/.test(field)) {
					this._indexedMapFields.push(field);
				}
			}
		}
		this._indexedMapFields = _.uniq(this._indexedMapFields);
	}

	/**
	 * Convert an index, given a path and a value
	 *
	 * @method _convertIndex
	 * @private
	 * @param {String} path - Field path for this index.
	 * @param {Mixed} value - The value of this index.
	 * @param {Object} [subschema]
	 * @return {Object} A mongo index object.
	 * @since v0.5.0
	 */
	_convertIndexSpec(path, value, subschema) {
		let specIndex = MongoModel._convertIndexType(value, subschema);
		// If this is an array path, remove the array index parts
		let specPath = path.replace(/\.\$/g, '');
		if (this.getSchema().hasParentType(path, 'map')) {
			// If this is a map, we need to make special indexes
			if (specIndex !== 1) {
				let msg = 'Cannot index map contents with anything but exact matches';
				throw new XError(XError.INTERNAL_ERROR, msg);
			}
			let parts = specPath.split('.');
			let field = parts.pop();
			let mapPath = parts.join('|');
			specPath = `_mapidx_${mapPath}^${field}`;
		}
		return { specPath, specIndex };
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
	 * @since v0.0.1
	 */
	_getSchemaIndices() {
		let allIndices = [];

		// Crawl the schema to find any indexed single fields
		let model = this;
		this.schema.traverseSchema({
			onSubschema(subschema, path/*, subschemaType*/) {
				if (!subschema.index && !subschema.unique) { return; }
				let { specPath, specIndex } = model._convertIndexSpec(path, subschema.index || true, subschema);
				let indexEntry = {
					spec: {
						[`${specPath}`]: specIndex
					},
					options: {}
				};
				if (subschema) {
					if (subschema.sparse) {
						indexEntry.options.sparse = true;
					}
					if (subschema.unique) {
						indexEntry.options.unique = true;
					}
				}
				allIndices.push(indexEntry);
			}
		}, {
			includePathArrays: true
		});

		return allIndices;
	}

	/**
	 * Returns all indices on this model.
	 *
	 * @method getIndices
	 * @return {Object[]} - Each entry is in the form:
	 *   `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
	 * @since v0.0.1
	 */
	getIndices() {
		return this._indices;
	}

	/**
	 * Returns all fields used for indexing map values.
	 *
	 * @method getIndexedMapFields
	 * @return {String[]} - The map fields following the following form:
	 *   `_mapidx_${map}|${path}^${field1}^${field2}...`
	 * @since v0.5.0
	 */
	getIndexedMapFields() {
		return this._indexedMapFields;
	}

	/**
	 * Get the model's keys
	 *
	 * @method getKeys
	 * @return {Array{String}}
	 * @since v0.0.1
	 */
	getKeys() {
		// Return defined and/or cached keys
		if (this.keys) return this.keys;

		let keys = [];

		// Find fields marked as keys
		this.schema.traverseSchema({
			onSubschema(subschema, path/*, subschemaType*/) {
				if (subschema.key) keys.push(path);
			}
		});

		// If no explicit keys, find the largest index
		if (_.isEmpty(keys)) {
			let indices = this.getIndices();

			indices.forEach((index) => {
				let indexKeys = _.keys(index.spec);
				if (indexKeys.length > keys.length) {
					let areSimpleIndices = _.every(index.spec, (indexValue) => {
						return (indexValue === 1 || indexValue === -1);
					});

					if (areSimpleIndices) keys = indexKeys;
				}
			});

			keys.reverse();
		}

		// If still no keys, throw an exception
		if (_.isEmpty(keys)) {
			throw new XError(XError.NOT_FOUND, 'No keys can be determined.');
		}

		// Cache the computed keys
		this.keys = keys;
		return keys;
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
	 * @since v0.0.1
	 */
	_ensureExists(create = true) {
		let db;
		return this.dbPromise
			.then((_db) => {
				db = _db;
				return new Promise((resolve, reject) => {
					db.collection(
						this.getCollectionName(),
						objtools.merge({}, this.collectionOptions, { strict: true }),
						function(err, collection) {
							if (err) return reject(MongoError.fromMongoError(err));

							resolve(collection);
						}
					);
				});
			})
			.catch((err) => {
				if (/does not exist/.test(err.message) && create) {
					// Collection doesn't yet exist.  Create it.
					return new Promise((resolve, reject) => {
						db.createCollection(
							this.getCollectionName(),
							objtools.merge({}, this.collectionOptions, { strict: true }),
							function(err, collection) {
								if (err) return reject(MongoError.fromMongoError(err));

								resolve(collection);
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
	 * @param {mongodb.Collection} collection - The Mongo native driver collection
	 * @return {Promise} - Resolve with collection or rejects with XError
	 * @since v0.0.1
	 */
	_ensureIndices(collection) {
		return pasync.eachSeries(this.getIndices(), (indexInfo) => {
			return new Promise((resolve, reject) => {
				collection.createIndex(indexInfo.spec, indexInfo.options, (err) => {
					if (err) return reject(MongoError.fromMongoError(err));
					resolve();
				});
			});
		})
		.then(() => collection);
	}

	/**
	 * Initializes the Mongo collection.  This normally happens automatically on construction
	 * unless the `initialize` option was set to false.
	 *
	 * @method initCollection
	 * @return {Promise{mongodb.MongoCollection}} - Returns this.collection
	 * @since v0.0.1
	 */
	initCollection() {
		if (this._startedInitializing) {
			return this.collectionPromise;
		}
		this._startedInitializing = true;

		let collection;
		return this._ensureExists()
			.then((_collection) => collection = _collection)
			.then((collection) => this._ensureIndices(collection))
			.then((collection) => collection.indexes())
			.then((indexes) => {
				this.indexes = indexes;
				return collection;
			})
			.then(this.collectionPromiseResolve)
			.catch((err) => {
				this.collectionPromiseReject(err);
				this.db.emit(
					'error',
					new XError(XError.DB_ERROR, 'Error initializing collection ' + this.getCollectionName(), err)
				);
			})
			.catch(pasync.abort)
			.then(() => this.collectionPromise);
	}

	/**
	 * Get model name
	 *
	 * @method getName
	 * @return {String}
	 * @since v0.0.1
	 */
	getName() {
		return this.modelName;
	}

	/**
	 * Get collection name, which is the camelCase variant of `modelName`.
	 *
	 * @method getCollectionName
	 * @return {String}
	 * @since v0.2.0
	 */
	getCollectionName() {
		return _.camelCase(this.getName());
	}

	/**
	 * Create a MongoDocument
	 *
	 * @method _createExisting
	 * @private
	 * @param {Object} data
	 * @param {Object} options
	 * @return {MongoDocument}
	 * @since v0.0.1
	 */
	create(data = {}, options = {}) {
		return new MongoDocument(this, data, options);
	}

	/**
	 * Create a MongoDocument that represents an existing document in the database.
	 * The data block given must include an _id field.
	 *
	 * @method _createExisting
	 * @private
	 * @param {Object} data
	 * @param {Object} options
	 * @return {MongoDocument}
	 * @since v0.0.1
	 */
	_createExisting(data = {}, options = {}) {
		options.isExisting = true;
		return this.create(data, options);
	}

	/**
	 * Find records in database
	 *
	 * @method find
	 * @param {commonQuery.Query} query - Query for records to find
	 * @param {Object} [options={}] - Mongo options
	 * @return {Array{MongoDocument}} - List of result documents
	 * @since v0.0.1
	 */
	find(query, options = {}) {
		let isPartial = !!options.fields;

		// Transform the query according to the schema
		query = this.normalizeQuery(query);

		let cursor;
		return this.collectionPromise
			.then((collection) => MongoModel._findWithOptions(collection, query.getData(), options))
			.then((mongoCursor) => {
				cursor = mongoCursor;
				return cursor.toArray();
			})
			.then((results) => {
				return _.map(results, (data) => this._createExisting(data, { isPartial }));
			})
			.then((results) => {
				if ('total' in options) {
					return cursor.count()
						.then((total) => {
							results.total = total;
							return results;
						});
				}

				return results;
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
		let isPartial = !!options.fields;

		// Transform the query according to the schema
		query = this.normalizeQuery(query);

		// Run the query, streaming the results
		let stream = new CursorResultStream(this, null, { isPartial });
		this.collectionPromise
			.then((collection) => {
				let cursor = MongoModel._findWithOptions(collection, query.getData(), options);
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
	 * @return {MongoDocument} - Result document
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
	 * @return {Array{MongoDocument}} - List of result documents
	 * @since v0.0.1
	 */
	insertMulti(datas, options = {}) {
		// Transform the mongo options
		let mongoOptions = MongoModel._transformMongoOptions(options, this.db);

		// Normalize the data according to the schema
		for (let data of datas) {
			this.schema.normalize(data, options);
			this.normalizeDocumentIndexedMapValues(data);
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
					throw new MongoError(XError.DB_ERROR, 'Unexpected insert result', { result: result.result });
				}
				return _.map(result.ops, (data) => this._createExisting(data));
			}, (err) => {
				throw MongoError.fromMongoError(err);
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
		let mongoOptions = MongoModel._transformMongoOptions(options, this.db);

		// Transform the query according to the schema
		query = this.normalizeQuery(query);

		return this.collectionPromise
			.then((collection) => collection.count(query.getData(), mongoOptions));
	}

	/**
	 * Perform multiple aggregates on the database
	 *
	 * @method aggregateMulti
	 * @param {commonQuery.Query} query - Query for records on which to perform the aggregates
	 * @param {Object{Object}|Object{commonQuery.Aggregate}} aggregates - Table of aggregate queries,
	 *   where the key is the user-defined name of the aggregate, and the value is a commonQuery aggregate
	 * @param {Object} [options={}] - Mongo options
	 * @return {Promise{Array{Object}}} - Resolves to table of aggregate results, in the commonQuery syntax
	 * @since v0.1.0
	 */
	aggregateMulti(query, aggregates, options = {}) {
		// Transform the query and aggregates according to the schema
		query = this.normalizeQuery(query);
		for (let key in aggregates) {
			aggregates[key] = this.normalizeAggregate(aggregates[key]);
		}

		let pipelines = aggregateUtils.createAggregatePipelines(this.schema, query, aggregates);

		return this.collectionPromise
			.then((collection) => {
				let results = pipelines.map((pipelineData) => {
					return collection.aggregate(pipelineData.pipeline, options).toArray()
						.catch((err) => { throw MongoError.fromMongoError(err); })
						.then((results) => {
							pipelineData.results = results;
							return pipelineData;
						});
				});

				return Promise.all(results)
					.then((pipelines) => {
						return aggregateUtils.createAggregateResult(this.schema, pipelines, aggregates);
					});
			});
	}

	/**
	 * Remove records from database
	 *
	 * @method remove
	 * @param {commonQuery.Query} query - Query for records to remove
	 * @param {Object} [options={}] - Mongo options
	 * @return {Object} - The response from the mongo command
	 * @since v0.0.1
	 */
	remove(query, options = {}) {
		// Transform the mongo options
		let mongoOptions = MongoModel._transformMongoOptions(options, this.db);

		// Transform the query according to the schema
		query = this.normalizeQuery(query);

		return this.collectionPromise
			.then((collection) => collection.remove(query.getData(), mongoOptions));
	}

	/**
	 * Update records in database
	 *
	 * @method update
	 * @param {commonQuery.Query} query - Query for records to update
	 * @param {commonQuery.Update} update - Update query
	 * @param {Object} [options={}] - Mongo options
	 * @return {Promise} - Resolves with the number of documents updated, or rejects with XError
	 * @since v0.0.1
	 */
	update(query, update, options = {}) {
		// Transform the mongo options
		let mongoOptions = MongoModel._transformMongoOptions(options, this.db);
		let updateOptions = _.pick(options, [ 'skipFields' ]);

		// Transform the query and update according to the schema
		update = this.normalizeUpdate(update);

		// Check if this is accessing a field inside a map
		let updateFields = update.getUpdatedFields();
		let isUpdatingMap = false;
		for (let field of updateFields) {
			if (this.getSchema().hasParentType(field, 'map')) {
				isUpdatingMap = true;
				break;
			}
		}
		if (!isUpdatingMap) {
			// We can run a normal update on this, since it's not touching map data
			query = this.normalizeQuery(query);
			return this.collectionPromise
				.then((collection) => collection.update(query.getData(), update.getData(), mongoOptions))
				.then((result) => result.result.nModified);
		} else {
			// We need to rebuild map data, so run this as a streaming, in memory save
			let numUpdated = 0;
			return this.findStream(query, mongoOptions).each((doc) => {
				update.apply(doc.getData(), updateOptions);
				return doc.save()
					.then(() => numUpdated++);
			}).intoPromise()
				.then(() => numUpdated);
		}
	}

	/**
	 * Updates all documents matching a given query if they exist, and otherwise creates one.
	 *
	 * @method upsert
	 * @param {commonQuery.Query} query - Query for records to update
	 * @param {commonQuery.Update} update - Update query
	 * @param {Object} [options={}] - Mongo options
	 * @return {Promise} - Resolves with the number of documents updated, or rejects with XError
	 * @since v0.5.0
	 */
	upsert(query, update, options = {}) {
		options.upsert = true;
		return this.update(query, update, options);
	}

	/**
	 * Normalizes and validates the query that is passed in.
	 *
	 * @method normalizeQuery
	 * @param {Query|Object} query - Query to normalize.
	 * @param {Object} [options] - Additional options to pass to the common-query normalizer.
	 * @return {Query} - The query object after normalization.
	 * @since v0.5.0
	 */
	normalizeQuery(query, options) {
		query = super.normalizeQuery(query, options);

		// Try to normalize fields in the query
		this._normalizeQueryMapFields(query.getData());

		return query;
	}

	/**
	 * Normalize query data map fields in place.
	 *
	 * @method _normalizeMapFieldQuery
	 * @param {Object} data - Raw query data to normalize.
	 * @since v0.5.0
	 */
	_normalizeQueryMapFields(data) {
		let mapPath = null;
		let mapFields = [];
		// Try to get map fields out of the data obejct
		for (let key in data) {
			if (key[0] === '$') {
				if (key === '$and' || key === '$or' || key === '$nor') {
					// Recurse on array entries
					for (let subdata of data[key]) {
						this._normalizeQueryMapFields(subdata);
					}
				}
				continue;
			}
			let hasParentMap;
			try {
				hasParentMap = this.getSchema().hasParentType(key, 'map');
			} catch (e) {
				hasParentMap = false;
			}
			if (!hasParentMap) { continue; }

			let pathComponents = key.split('.');
			let keyField = pathComponents.pop();
			let keyMapPath = pathComponents.join('.');

			if (mapPath === null) {
				mapPath = keyMapPath;
			} else if (mapPath !== keyMapPath) {
				// We can't do any indexing across multiple maps/keys
				return;
			}
			// Push the field
			mapFields.push(keyField);
		}
		if (mapFields.length === 0) {
			// If no mapFields, then there's nothing to try to optimize
			return;
		}
		mapFields.sort();

		// Ensure all fields contain only valid operators
		let validOperators;
		if (mapFields.length === 1) {
			validOperators = [ '$eq', '$lte', '$lt', '$gte', '$gt' ];
		} else {
			validOperators = [ '$eq' ];
		}
		for (let field of mapFields) {
			let fieldValue = data[field];
			if (_.isPlainObject(fieldValue)) {
				let foundInvalid = false;
				for (let operator in fieldValue) {
					if (!_.contains(validOperators, operator)) {
						foundInvalid = true;
						break;
					}
				}
				// Found an invalid operator, just do table scan
				if (foundInvalid) { return; }
			}
		}

		// Get contracted map path (with keys extracted, and the start of the specPath for the map)
		let { specPath, keys } = this._getContractedMapPath(mapPath);
		if (!keys.length) { return; }

		// Append fields we want to the specPath
		for (let mapField of mapFields) {
			specPath += `^${mapField}`;
		}
		// Ensure the map we're lookg for is actually indexed
		let foundIndex = false;
		for (let index of this.getIndices()) {
			let indexSpecPaths = _.keys(index.spec);
			if (indexSpecPaths.length !== 1) { continue; }
			let [ indexSpecPath ] = indexSpecPaths;
			if (indexSpecPath === specPath) {
				foundIndex = true;
				break;
			}
		}
		if (!foundIndex) {
			// This map path isn't indexed
			return;
		}

		if (mapFields.length === 1) {
			// This is a single map field. Extra operators are allowed
			let [ mapField ] = mapFields;
			let field = `${mapPath}.${mapField}`;
			let fieldValue = data[field];
			if (_.isPlainObject(fieldValue) && fieldValue.$eq !== undefined) {
				// Collapse $eq
				fieldValue = fieldValue.$eq;
			}
			if (_.isPlainObject(fieldValue)) {
				// Handle $lte/$lt/$gte/$gt
				let loOp, loVal, hiOp, hiVal;
				if (fieldValue.$lt || fieldValue.$lte) {
					hiOp = (fieldValue.$lte) ? '$lte' : '$lt';
					hiVal = BSON.serialize(keys.concat([ fieldValue[hiOp] ]));
				}
				if (fieldValue.$gt || fieldValue.$gte) {
					loOp = (fieldValue.$gte) ? '$gte' : '$gt';
					loVal = BSON.serialize(keys.concat([ fieldValue[loOp] ]));
				}
				if (loOp === undefined && hiOp === undefined) {
					// Something weird is happening. Bomb out!
					return;
				}
				if (loOp === undefined) {
					loOp = '$gte';
					loVal = BSON.serialize(keys);
					loVal[0] = hiVal[0];
				} else if (hiOp === undefined) {
					hiOp = '$lt';
					hiVal = BSON.serialize(keys);
					hiVal[0] = loVal[0];
					// Need to add one bit to the hiVal buffer
					let i = hiVal.length - 2;
					while (i > 0 && hiVal[i] >= 127) {
						hiVal[i] = 0;
						i -= 1;
					}
					if (i < 0) {
						// This hsould never happen. BSON value is already maxed out.
						// Quit out, since something bigger is happening.
						return;
					}
					hiVal[i] += 1;
				}
				// Add the range match against the indexed map
				data[specPath] = {
					[loOp]: loVal.toString(),
					[hiOp]: hiVal.toString()
				};

			} else {
				// Add the exact match against the indexed map
				keys.push(fieldValue);
				data[specPath] = BSON.serialize(keys).toString();

			}
			// Delete field, since we are converting into an indexed map value
			delete data[field];

		} else {
			// Only exact matches are possible
			for (let mapField of mapFields) {
				let field = `${mapPath}.${mapField}`;
				let fieldValue = data[field];
				// Delete fields, since we are collapsing them into one indexed map value
				delete data[field];
				if (_.isPlainObject(fieldValue)) {
					// Collapse $eq
					fieldValue = fieldValue.$eq;
				}
				keys.push(fieldValue);
			}
			// Add the value to the indexed map
			data[specPath] = BSON.serialize(keys).toString();
		}
	}

	/**
	 * Contract the given path into a field value into a specPath and keys.
	 * NOTE: this method is memoized, so traversals only need to happen once per thread.
	 *
	 * @method _getContractedMapPath
	 * @private
	 * @param {String} path - Full map field path.
	 * @return {Object} With properties `specPath` and `keys`.
	 * @since v0.5.0
	 */
	_getContractedMapPath(path) {
		if (!this.__memoizedGetContractedMapPath) {
			this.__memoizedGetContractedMapPath = {};
		} else if (this.__memoizedGetContractedMapPath[path]) {
			return objtools.deepCopy(this.__memoizedGetContractedMapPath[path]);
		}

		let components = path.split('.');
		let specPath = '';
		let subpath = '';
		let keys = [];
		while (components.length > 0) {
			let component = components.shift();
			specPath = ((specPath) ? `${specPath}.` : '') + component;
			subpath = ((subpath) ? `${subpath}.` : '') + component;
			let subschema = this.getSchema().getSubschemaData(subpath);
			if (!subschema) { return { specPath: '', keys: [] }; }
			if (subschema.type === 'map') {
				keys.push(components.shift());
				subpath += '.$';
			}
		}
		specPath = `_mapidx_${specPath.replace('.', '|')}`;

		let ret = { specPath, keys };
		this.__memoizedGetContractedMapPath[path] = ret;
		return objtools.deepCopy(ret);
	}

	/**
	 * Expand subsection of raw data into a "map field" object, containing an `expandedPath` and `keys`
	 * properties.
	 *
	 * @method _getExpandedMapPaths
	 * @private
	 * @param {Object} data - Current view of the normalized data.
	 * @param {String} path - Visited path in the normalized data.
	 * @param {String[]} components - Path components we still need to visit.
	 * @return {Object} With properties `expandedPath` (path to get a field value) and
	 *   `keys` (map keys found along this path).
	 * @since v0.5.0
	 */
	_getExpandedMapPaths(data, path, components) {
		let component = components[0];
		let subcomponents = components.slice(1);

		let subdata = data[component];
		let subpath = ((path) ? `${path}.` : '') + component;
		let subschema = this.getSchema().getSubschemaData(subpath);
		if (!subdata || !subschema) { return []; }
		let expandedPaths = [];
		if (subcomponents.length) {
			if (subschema.type === 'map') {
				for (let key in subdata) {
					let childExpandedPaths = this._getExpandedMapPaths(subdata[key], `${subpath}.$`, subcomponents);
					for (let { expandedPath, keys } of childExpandedPaths) {
						expandedPaths.push({
							expandedPath: `${component}.${key}.${expandedPath}`,
							keys: [ key ].concat(keys)
						});
					}
				}
			} else {
				let childExpandedPaths = this._getExpandedMapPaths(subdata, subpath, subcomponents);
				for (let { expandedPath, keys } of childExpandedPaths) {
					expandedPaths.push({
						expandedPath: `${component}.${expandedPath}`,
						keys
					});
				}
			}
		} else {
			if (subschema.type === 'map') {
				// Add all the new map fields!
				for (let key in subdata) {
					expandedPaths.push({
						expandedPath: `${component}.${key}`,
						keys: [ key ]
					});
				}
			} else {
				expandedPaths.push({
					expandedPath: `${component}`,
					keys: []
				});
			}
		}
		return expandedPaths;
	}

	/**
	 * Normalize the data inside indexed map fields, which get converted to arrays of stringified BSON
	 * data.
	 *
	 * @method normalizeDocumentIndexedMapValues
	 * @param {Object} data - The raw docuemnt data to normalize.
	 * @since v0.5.0
	 */
	normalizeDocumentIndexedMapValues(data) {
		let indexedMapFields = this.getIndexedMapFields();

		for (let indexedMapField of indexedMapFields) {
			let mapField = indexedMapField.substring(8);
			// Split on '_' field identifier
			let [ mapPath, ...mapFields ] = mapField.split('^');
			let components = mapPath.split('|');
			let expandedPaths = this._getExpandedMapPaths(data, '', components);
			for (let { expandedPath, keys } of expandedPaths) {
				let indexData = objtools.deepCopy(keys);
				let allFound = true;
				//TODO: support sparse indexes, and non-field indexes
				for (let field of mapFields) {
					let fieldPath = (expandedPath ? `${expandedPath}.` : '') + field;
					let value = objtools.getPath(data, fieldPath);
					if (value === undefined || value === null) {
						allFound = false;
						break;
					}
					indexData.push(value);
				}
				if (allFound) {
					// Add the bson indexed field to data
					if (!data[indexedMapField]) { data[indexedMapField] = []; }
					data[indexedMapField].push(BSON.serialize(indexData).toString());
				}
			}
		}
	}
}

module.exports = MongoModel;
