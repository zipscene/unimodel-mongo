const SchemaModel = require('zs-unimodel').SchemaModel;
const objtools = require('zs-objtools');
const pasync = require('pasync');
const XError = require('xerror');
const UnimongoError = require('./unimongo-error');
const UnimongoDocument = require('./unimongo-document');
const commonQuery = require('zs-common-query');
const CursorResultStream = require('./cursor-result-stream');
const _ = require('lodash');

const multiFieldValueSeparator = ' |-| ';

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

// Construct an expression that converts the given expression result into a string
const toStringExpr = function(expr) {
	return { $substr: [ expr, 0, 256 ] };
};

/**
 * MongoDB model class for Unimodel.
 *
 * @class UnimongoModel
 * @constructor
 * @param {String} collectionName
 * @param {Object} schema
 * @param {Object} db
 * @param {Object} [options] - Additional options
 *   @param {Array{String}} options.keys - List of keys for the model
 *   @param {Boolean} options.allowSavingPartials - Whether to allow saving partial documents
 * @since v0.0.1
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
		if (typeof this.options.allowSavingPartials === 'undefined') this.options.allowSavingPartials = true;

		this.keys = this.options.keys;

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
		if (this.options.initialize !== false) {
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
	 * @since v0.0.1
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
	 * @since v0.0.1
	 */
	getIndices() {
		return this._indices;
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
			onSubschema(subschema, path, subschemaType) {
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
						this.collectionName,
						objtools.merge({}, this.collectionOptions, { strict: true }),
						function(err, collection) {
							if (err) return reject(UnimongoError.fromMongoError(err));

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
							this.collectionName,
							objtools.merge({}, this.collectionOptions, { strict: true }),
							function(err, collection) {
								if (err) return reject(UnimongoError.fromMongoError(err));

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
	 * @param {Collection} collection - The Mongo native driver collection
	 * @return {Promise} - Resolve with collection or rejects with XError
	 * @since v0.0.1
	 */
	_ensureIndices(collection) {
		return pasync.eachSeries(this.getIndices(), (indexInfo) => {
			return new Promise((resolve, reject) => {
				collection.createIndex(indexInfo.spec, indexInfo.options, (err) => {
					if (err) return reject(UnimongoError.fromMongoError(err));
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
	 * @return {Promise{MongoCollection}} - Returns this.collection
	 * @since v0.0.1
	 */
	initCollection() {
		if (this._startedInitializing) {
			return this.collectionPromise;
		}
		this._startedInitializing = true;

		let collection;
		this._ensureExists()
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
					new XError(XError.DB_ERROR, 'Error initializing collection ' + this.collectionName, err)
				);
			})
			.catch(pasync.abort);

		return this.collectionPromise;
	}

	/**
	 * Get collection name
	 *
	 * @method getName
	 * @return {String}
	 * @since v0.0.1
	 */
	getName() {
		return this.collectionName;
	}

	/**
	 * Create a UnimongoDocument
	 *
	 * @method _createExisting
	 * @private
	 * @param {Object} data
	 * @param {Object} options
	 * @return {UnimongoDocument}
	 * @since v0.0.1
	 */
	create(data = {}, options = {}) {
		return new UnimongoDocument(this, data, options);
	}

	/**
	 * Create a UnimongoDocument that represents an existing document in the database.
	 * The data block given must include an _id field .
	 *
	 * @method _createExisting
	 * @private
	 * @param {Object} data
	 * @param {Object} options
	 * @return {UnimongoDocument}
	 * @since v0.0.1
	 */
	_createExisting(data = {}, options = {}) {
		options.isExisting = true;
		return this.create(data, options);
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
		let isPartial = !!options.fields;

		// Transform the query according to the schema
		if (!_.isFunction(query.normalize)) query = commonQuery.createQuery(query);

		query.normalize({ schema: this.schema });

		return this.collectionPromise
			.then((collection) => UnimongoModel._findWithOptions(collection, query.getData(), options).toArray())
			.then((results) => {
				return _.map(results, (data) => this._createExisting(data, { isPartial }));
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
		if (!_.isFunction(query.normalize)) {
			query = commonQuery.createQuery(query);
		}
		query.normalize({ schema: this.schema });

		// Run the query, streaming the results
		let stream = new CursorResultStream(this, null, { isPartial });
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

	// Function to return a $group expression for each aggregate type
	_makeGroupExpression(spec) {
		if (_.has(spec, 'stats')) {
			return {
				idExpr: null,
				fields: []
			};
		} else if (!_.has(spec, 'groupBy')) {
			throw new XError(XError.UNSUPPORTED_OPERATION, 'Aggregate type is not supported.');
		}

		let groupBy = spec.groupBy;
		if (!_.isArray(groupBy)) groupBy = [ groupBy ];

		// Construct an array of aggregate expressions that generate the value to group by for each component
		let groupValueSpecs = groupBy.map((groupBySpec) => {
			if (typeof groupBySpec == 'string') {
				groupBySpec = { field: groupBySpec };
			}
			if (!groupBySpec || typeof groupBySpec != 'object' || !groupBySpec.field || typeof groupBySpec.field != 'string') {
				throw new XError(
					XError.BAD_REQUEST,
					'A groupBy spec must contain a string "field" parameter',
					{ groupBy, groupBySpec }
				);
			}
			let field = groupBySpec.field;
			let expr;
			if (groupBySpec.ranges) {
				expr = makeRangesExpression(groupBySpec.ranges, field);
			} else if (groupBySpec.dateRanges) {
				expr = makeRangesExpression(groupBySpec.dateRanges, field);
			} else if (groupBySpec.interval) {
				expr = makeIntervalExpression(groupBySpec.interval, field);
			} else if (groupBySpec.timeInterval) {
				expr = makeTimeIntervalExpression(groupBySpec.timeInterval, field);
			} else {
				expr = '$' + field;
			}

			return {
				field,
				valueExpr: expr
			};
		});

		// Construct the expression to generate the _id
		let groupValueExpr;
		if (groupValueSpecs.length === 0) {
			throw new XError(XError.UNSUPPORTED_OPERATION, 'No grouping expressions found');
		} else if (groupValueSpecs.length === 1) {
			groupValueExpr = toStringExpr(groupValueSpecs[0].valueExpr);
		} else {
			groupValueExpr = {
				$concat: []
			};
			groupValueSpecs.forEach((spec, idx) => {
				if (idx !== 0) groupValueExpr.$concat.push(multiFieldValueSeparator);
				groupValueExpr.$concat.push(toStringExpr(spec.valueExpr));
			});
		}

		return {
			idExpr: groupValueExpr,
			fields: groupValueSpecs
		};
	}

	_createAggregatePipelines(query, specs) {
		let aggregatesByGroup = {};

		_.forEach(specs, (spec, specIndex) => {
			let groupExpression = this._makeGroupExpression(spec);
			let groupHash = objtools.objectHash(groupExpression.idExpr);

			if (aggregatesByGroup[groupHash]) {
				aggregatesByGroup[groupHash].specIndices.push(specIndex);
			} else {
				aggregatesByGroup[groupHash] = {
					groupExpression,
					specIndices: [ specIndex ]
				};
			}
		});

		// For each group expression, construct a set of fields to return
		_.forEach(aggregatesByGroup, (groupExpressionData) => {
			let aggregateFields = {};

			groupExpressionData.specIndices.forEach((aggregateIndex) => {
				let aggregateSpec = specs[aggregateIndex];

				// Add field passthroughs
				groupExpressionData.groupExpression.fields.forEach((fieldExprSpec, fieldIdx) => {
					aggregateFields[`agg_${aggregateIndex}_key_${fieldIdx}`] = { $first: fieldExprSpec.valueExpr };
				});

				// Add aggregate fields to the pipeline
				if (aggregateSpec.stats) {
					_.forEach(aggregateSpec.stats, (stat, statsField) => {
						if (stat.count) {
							aggregateFields[`agg_${aggregateIndex}_count_${statsField}`] = {
								$sum: {
									$cond: [
										{
											$eq: [
												{
													$ifNull: [ `$${statsField}`, null ]
												},
												null
											]
										},
										0,
										1
									]
								}
							};
						}
						if (stat.avg) aggregateFields[`agg_${aggregateIndex}_avg_${statsField}`] = { $avg: `$${statsField}` };
						if (stat.min) aggregateFields[`agg_${aggregateIndex}_min_${statsField}`] = { $min: `$${statsField}` };
						if (stat.max) aggregateFields[`agg_${aggregateIndex}_max_${statsField}`] = { $max: `$${statsField}` };
					});
				}

				if (aggregateSpec.total) aggregateFields[`agg_${aggregateIndex}_total`] = { $sum: 1 };
			});

			groupExpressionData.fields = aggregateFields;
		});

		// Construct an aggregate pipeline for each group by expression
		let pipelines = [];
		_.forEach(aggregatesByGroup, (groupExpressionData) => {
			groupExpressionData.fields._id = groupExpressionData.groupExpression.idExpr;
			groupExpressionData.pipeline = [
				{ $match: query },
				{ $group: groupExpressionData.fields }
			];
			pipelines.push(groupExpressionData);
		});

		return pipelines;
	}

	_createAggregateResult(pipelines, specs) {
		let aggregateResults = {};

		pipelines.forEach((pipelineData) => {
			let results = pipelineData.results;
			let keyPattern = /^agg_([A-Za-z0-9-]+)_([A-Za-z0-9-]+)(?:_(.*))?$/;

			for(let resultEntryIdx = 0; resultEntryIdx < results.length; resultEntryIdx++) {
				let resultEntry = results[resultEntryIdx];
				let resultsByAggregate = {};

				// Extract the fields for each aggregate from the pipeline result
				for(let resultKey in resultEntry) {
					let matches = keyPattern.exec(resultKey);
					if (matches) {
						let [ , matchGroup, matchOperator, matchField ] = matches;
						if (!resultsByAggregate[matchGroup]) resultsByAggregate[matchGroup] = { stats: {} };
						if (matchField) {
							if (!resultsByAggregate[matchGroup].stats[matchField]) {
								resultsByAggregate[matchGroup].stats[matchField] = {};
							}
							resultsByAggregate[matchGroup].stats[matchField][matchOperator] = resultEntry[resultKey];
						} else {
							resultsByAggregate[matchGroup][matchOperator] = resultEntry[resultKey];
						}
					}
				}

				// Reformat the key fields in each entry according to the type of grouping requested
				for(let aggregateName in resultsByAggregate) {
					let aggrResultEntry = resultsByAggregate[aggregateName];
					let aggrSpec = specs[aggregateName];
					let groupBy = aggrSpec.groupBy;

					if (groupBy) {
						if (!_.isArray(groupBy)) groupBy = [ groupBy ];

						let keyArray = [];

						for(let keyIdx = 0; keyIdx < groupBy.length; keyIdx++) {
							let keyValue = aggrResultEntry['key_' + keyIdx];
							delete aggrResultEntry['key_' + keyIdx];
							if (keyValue === undefined || keyValue === null) {
								keyArray.push(null);
							} else if (typeof groupBy[keyIdx] == 'string' || groupBy[keyIdx].ranges || groupBy[keyIdx].interval || groupBy[keyIdx].dateRanges) {
								keyArray.push(keyValue);
							} else if (groupBy[keyIdx].timeInterval) {
								keyArray.push(formatTimeIntervalValue(keyValue));
							} else {
								keyArray.push(keyValue);
							}
						}

						aggrResultEntry.key = keyArray;

						if (!aggregateResults[aggregateName]) aggregateResults[aggregateName] = [];
						aggregateResults[aggregateName].push(aggrResultEntry);
					} else {
						aggregateResults[aggregateName] = aggrResultEntry;
					}
				}
			}
		});

		return aggregateResults;
	}

	aggregateMulti(query, specs, options = {}) {
		let pipelines = this._createAggregatePipelines(query, specs);

		// TODO: condense applicable groups
		// TODO: handle errors
		// TODO: parse results

		return this.collectionPromise
			.then((collection) => {
				let results = pipelines.map((pipelineData) => {
					return collection.aggregate(pipelineData.pipeline).toArray()
						.catch((err) => { throw UnimongoError.fromMongoError(err); })
						.then((results) => {
							pipelineData.results = results;
							return pipelineData;
						});
				});

				return Promise.all(results)
					.then((pipelines) => {
						return this._createAggregateResult(pipelines, specs);
					});
			});
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
	 * @return {Object} - The response from the mongo command
	 * @since v0.0.1
	 */
	update(query, update, options = {}) {
		// Transform the mongo options
		let mongoOptions = UnimongoModel._transformMongoOptions(options);

		// Transform the query and update according to the schema
		if (!_.isFunction(query.normalize)) query = commonQuery.createQuery(query);
		if (!_.isFunction(update.normalize)) update = commonQuery.createUpdate(update);

		query.normalize({ schema: this.schema });
		update.normalize({ schema: this.schema });

		return this.collectionPromise
			.then((collection) => collection.update(query.getData(), update.getData(), mongoOptions));
	}

}

module.exports = UnimongoModel;
