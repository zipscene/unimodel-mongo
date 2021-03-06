// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const crypto = require('crypto');
const _ = require('lodash');
const pasync = require('pasync');
const XError = require('xerror');
const objtools = require('objtools');
const Profiler = require('simprof');
const { SchemaModel } = require('unimodel-core');
const MongoError = require('./mongo-error');
const { createSchema } = require('common-schema');
const MongoDocument = require('./mongo-document');
const aggregateUtils = require('./utils/aggregates');
const opUtils = require('./utils/ops');
const CursorResultStream = require('./cursor-result-stream');
const { PassThrough } = require('zstreams');
const bson = require('bson');
const { createQuery } = require('common-query');
const { coreDimensions: { LongLatDimension } } = require('polytoken');
const geolib = require('geolib');
const { queryFactory, updateFactory, aggregateFactory } = require('./common-query-factories');

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
 *   @param {String} options.uniqueIdField - Field name for the unique ID of documents in this model.
 *     This defaults to '_id'.  This is useful if you have a different ID field used, for example,
 *     as a shard key.  Operations such as updates query on this key to select a single document.
 *  @param {Boolean} options.neverSharded - By default, unimodel-mongo disallows or transforms operations
 *    that won't work the same in sharded environments (such as $near).  If this is set to true, such
 *    operations are allowed and not transformed.
 * @since v0.0.1
 */
class MongoModel extends SchemaModel {

	constructor(modelName, schema, db, options = {}) {
		// Initialize superclass
		options.queryFactory = queryFactory;
		options.updateFactory = updateFactory;
		options.aggregateFactory = aggregateFactory;
		super(schema, options);

		// Set up profiler
		this.profiler = new Profiler(`MongoModel - ${modelName}`);

		// Set class variables
		this.db = db;
		this.dbPromise = db.dbPromise;
		this.modelName = modelName;

		// Translate options
		this.options = options;
		if (!_.has(this.options, 'allowSavingPartials')) this.options.allowSavingPartials = true;
		if (!_.has(this.options, 'uniqueIdField')) this.options.uniqueIdField = '_id';
		if (!_.has(this.options, 'neverSharded')) this.options.neverSharded = false;

		this.keys = this.options.keys;

		// Map from our option names to mongo collection options
		this.collectionOptions = MongoModel._transformMongoOptions(options, this.db);

		// Set up the promise for returning this collection
		this.collectionPromise = new Promise((resolve, reject) => {
			this.collectionPromiseResolve = resolve;
			this.collectionPromiseReject = reject;
		});

		this._indexMapping = {};

		this._indexes = [];
		this._indexedMapFields = [];

		this._geoHashedIndexMapping = {};  // Map of shema field to information about geohashed index

		// Initialize the list of indexes on the collection
		// This contains elements in this format:
		// `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
		this._fillSchemaIndexes();
		// Precalculate the indexed map fields
		this._addIndexedMapFields(this._indexes);

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
	 * Converts the provided timeout to a value usable by mongo's maxTimeMS option.
	 *
	 * maxTimeMS accepts only positive integers, and zero is ignored. Timeouts are thus truncated to
	 * milliseconds, with a minumum value of 1.
	 *
	 * @private
	 * @static
	 * @param {Number} timeout - The timeout in seconds.
	 * @return {Number} - usable maxTimeMS value.
	 */
	static _convertTimeout(timeout) {
		return Math.max(Math.floor(timeout * 1000), 1);
	}

	/**
	 * Manually apply a set of post-query mongo operations (skip, limit, sort, project) onto an array
	 * of result data. This is used in cased when the standard mongo cursor is replaced with a special operation.
	 *
	 * @method _manualApplyQueryOptions
	 * @static
	 * @private
	 * @param {Array} data - Array of result data.
	 * @param {Mixed} options
	 */
	static _manualApplyQueryOptions(data, options) {
		// Sort
		if (Array.isArray(options.sort)) {
			data.sort(function(a, b) {
				for (let field of options.sort) {
					let reversed = false;
					if (field[0] === '-') {
						field = field.slice(1);
						reversed = true;
					}
					let valueA = objtools.getPath(a, field);
					let valueB = objtools.getPath(b, field);
					if (reversed) {
						if (valueA > valueB) return -1;
						if (valueB > valueA) return 1;
					} else {
						if (valueA > valueB) return 1;
						if (valueB > valueA) return -1;
					}
				}
				return 0;
			});
		}

		// Skip and limit
		if (_.isNumber(options.skip)) {
			data = data.slice(options.skip);
		}
		if (_.isNumber(options.limit)) {
			data = data.slice(0, options.limit);
		}

		// Filter fields
		if (Array.isArray(options.fields)) {
			let mask = {};
			for (let field of options.fields) {
				objtools.setPath(mask, field, true);
			}
			let fn = new objtools.ObjectMask(mask).createFilterFunc();
			data = _.map(data, fn);
		}

		return data;
	}

	/**
	 * Converts a user-specified index type into a mongo-compatible index type.
	 *
	 * @method _convertIndexType
	 * @private
	 * @static
	 * @param {Mixed} indexValue - User-supplied index type
	 * @param {Object} [subschema] - Subschema data for datatype (used for type hints)
	 * @return {Mixed} - Mongo index type
	 * @since v0.0.1
	 */
	static _convertIndexType(indexValue, subschema) {
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
			hashed: 'hashed',
			geoHashed: 'geoHashed'
		};

		let indexType, suppliedIndexType, indexParams = {};
		if (indexValue && typeof indexValue === 'object') {
			suppliedIndexType = indexValue.indexType;
		} else {
			suppliedIndexType = indexValue;
		}

		if (suppliedIndexType === true) {
			// Try to autodetermine the index type
			if (subschema && (subschema.type === 'geojson' || subschema.type === 'geopoint')) {
				indexType = '2dsphere';
			} else {
				indexType = 1;
			}
		} else if (indexTypeMap[suppliedIndexType]) {
			indexType = indexTypeMap[suppliedIndexType];
		} else {
			throw new XError(XError.INVALID_ARGUMENT, 'Invalid index type: ' + suppliedIndexType);
		}

		if (indexType === 'geoHashed') {
			indexParams.step = indexValue.step;
		}
		return { indexType, indexParams };
	}

	/**
	 * Translates a query that queries on a geoHashed field into one that uses the indexed polytokens, and
	 * executes that query.
	 *
	 * @method _translatedGeoHashed
	 * @static
	 * @private
	 * @param {mongodb.Collection} collection - The mongo native driver collection
	 * @param {Query} query - The commonQuery object
	 * @param {String} field - The field being queried on by $near
	 * @param {String} nearParams - The value of the $near query expression
	 * @param {MongoModel} model
	 * @param {Object} options
	 * @return {Promise{Object[]}}
	 */
	static _translatedGeoHashed(collection, query, field, nearParams, model, options) {
		let point = nearParams.$geometry;
		if (!point) {
			throw new XError(XError.INVALID_ARGUMENT, 'Query on geoHashed indexed field must provide $geometry');
		}
		let radius = nearParams.$maxDistance;
		if (!radius) {
			throw new XError(XError.INVALID_ARGUMENT, 'Query on geoHashed indexed field must provide $maxDistance');
		}
		if (nearParams.$minDistance) {
			throw new XError(XError.INVALID_ARGUMENT, 'Query on geoHashed indexed field cannot provide $minDistance');
		}
		let indexOptions = model._geoHashedIndexMapping[field];
		if (!indexOptions) throw new XError(XError.INTERNAL_ERROR, 'Could not find indexOptions for $near query');

		// Remove the $near from the query, and add the query on hash field
		query._traverse({
			exprOperator(exprValue, _field, operator, expr) {
				if (operator === '$near') {
					delete expr.$near;
					expr.$exists = true;
					return false;
				}
			}
		});
		let rangeTokens = indexOptions.dimension.getRangeTokens({ point, radius });
		let queryData = query.getData();
		queryData[indexOptions.field] = { $in: rangeTokens };

		let cursor = collection.find(queryData);
		// Send fields to query if provided
		if (options.fields) {
			if (!_.includes(options.fields, '__rev')) options.fields.push('__rev');
			let projection = {};
			for (let field of options.fields) {
				projection[field] = true;
			}
			projection[field] = true;
			projection[indexOptions.field] = true;
			projection._id = !options.no_id;
			cursor = cursor.project(projection);
		}
		// Add timeout to query if provided.
		if (_.isNumber(options.timeout)) {
			cursor = cursor.maxTimeMS(MongoModel._convertTimeout(options.timeout));
		}
		return cursor.toArray()
			.then((results) => {
				// Calculate minimum radius for each point in result documents, and remove those that dont qualify
				if (results.length === 0) return results;
				let normalCenter = indexOptions.dimension.normalizePoint(point);
				results = results.filter((data) => {
					model.schema.traverse(data, {
						onField: (travField, value, subschema) => {
							if (!value || subschema.type !== 'geopoint') return;
							if (travField.replace(/\.[0-9]+/g, '') !== field) return;
							// Calculate from circle center
							let normalTestPoint = indexOptions.dimension.normalizePoint(value);
							let geolibCenter = {
								longitude: normalCenter.coordinates[0],
								latitude: normalCenter.coordinates[1]
							};
							let geolibPoint = {
								longitude: normalTestPoint.coordinates[0],
								latitude: normalTestPoint.coordinates[1]
							};
							let distance = geolib.getDistance(geolibCenter, geolibPoint);
							if (typeof data._minNearDistance !== 'number' || distance < data._minNearDistance) {
								data._minNearDistance = distance;
							}
						}
					});
					if (typeof data._minNearDistance !== 'number' || data._minNearDistance > radius) return false;
					return true;
				});

				// Sort results by nearness if explicit sort is not provided
				if (!options.sort) {
					results.sort(function(a, b) {
						return a._minNearDistance - b._minNearDistance;
					});
				}
				// Remove internal state fields
				for (let data of results) {
					delete data._minNearDistance;
				}

				return MongoModel._manualApplyQueryOptions(results, options);
			});
	}

	/**
	 * Executes a find using options instead of the Mongo driver's chaining.
	 *
	 * @method _findWithOptions
	 * @static
	 * @param {mongodb.Collection} collection - The mongo native driver collection
	 * @param {Object} query - The query data
	 * @param {MongoModel} model
	 * @param {Object} options
	 *   @param {String} [options.operationId] - Tracking id for cancellation.
	 *   @param {Number} [options.timeout] - Maximum time for the operation, in seconds.
	 *   @param {Boolean} [options.canCursorTimeout=true] - Whether the cursor may time out.
	 *     This is the `timeout` option for mongodb.Cursor.
	 *   @param {Boolean} [options.no_id=false] - Whether to include `_id` in the projection.
	 *     Useful for making [covered queries](https://docs.mongodb.org/manual/core/query-optimization/#covered-query).
	 *   @param {Mixed} [options.hint] - Index hint to give mongodb.  Must be a mongodb index
	 *     name or mongodb-style index spec.
	 * @return {mongodb.Cursor} - Mongo native driver cursor
	 */
	static _findWithOptions(collection, query, model, options) {
		let cQuery = query;
		if (typeof cQuery.getOperators !== 'function') {
			cQuery = queryFactory.createQuery(objtools.deepCopy(query));
		}
		if (_.includes(cQuery.getOperators(), '$near')) {
			// Determine which alternative near query we will be running, if any
			let nearField, nearParams;
			cQuery._traverse({
				exprOperator(exprValue, field, operator) {
					if (operator === '$near') {
						if (nearField) {
							throw new XError(XError.INVALID_ARGUMENT, '$near can only be used once per query');
						}
						nearField = field;
						nearParams = exprValue;
					}
				}
			});
			if (!nearField || !nearParams) {
				throw new XError(XError.INTERNAL_ERROR, 'Expected $near');
			}

			// Check to make sure there are no $or's or $nor's in the query path to the $near
			let nearIsAnded = false;
			cQuery._traverse({
				queryOperator(exprValue, operator) {
					if (operator !== '$and') return false;
				},
				exprOperator(exprValue, field, operator) {
					if (operator === '$near') nearIsAnded = true;
				}
			});

			// Translate to geoHashed if the $near field has that type of index
			if (model._geoHashedIndexMapping[nearField]) {
				if (!nearIsAnded) {
					throw new XError(
						XError.INTERNAL_ERROR,
						'$near on a geohashed field can only be used at the root query level'
					);
				}
				return MongoModel._translatedGeoHashed(collection, cQuery, nearField, nearParams, model, options);
			}

			// If none of these apply, run a normal query without transformation
		}

		let cursorOptions = {
			tailable: options.tailable,
			awaitdata: options.awaitdata,
			numberOfRetries: options.numberOfRetries,
			tailableRetryInterval: options.tailableRetryInterval
		};
		// Add operationId as $comment, if any.
		query = opUtils.addComment(query, options.operationId);
		let cursor = collection.find(query, cursorOptions);
		// Workaround for awful MongoDB streams issue
		cursor.destroy = function(error) {
			if (error) this.emit('error', error);
			this.close();
		};

		if (options.canCursorTimeout === false) cursor.addCursorFlag('noCursorTimeout', true);
		if (_.isNumber(options.skip)) cursor = cursor.skip(options.skip);
		if (_.isNumber(options.limit)) cursor = cursor.limit(options.limit);

		if (_.isArray(options.fields)) {
			if (!_.includes(options.fields, '__rev')) options.fields.push('__rev');
			let projection = {};
			for (let field of options.fields) {
				projection[field] = true;
			}
			projection._id = !options.no_id;
			cursor = cursor.project(projection);
		} else {
			// Exclude map index fields, these are recalculated on save
			let projection = {};
			for (let field of model.getIndexedMapFields()) {
				projection[field] = 0;
			}
			cursor = cursor.project(projection);
		}

		if (_.isArray(options.sort)) {
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

		if (options.hint) {
			cursor = cursor.hint(options.hint);
		}

		if (_.isNumber(options.timeout)) {
			cursor = cursor.maxTimeMS(MongoModel._convertTimeout(options.timeout));
		}

		return cursor;
	}

	/**
	 * Create hash to use when storing indexes.
	 *
	 * @method createIndexHash
	 * @static
	 * @param {Array{String}} ...fields - An array of fields to use in the hash
	 * @return {String} - The index hash
	 * @since v0.8.0
	 */
	static createIndexHash(...fields) {
		let hash = crypto.createHash('sha1');

		for (let field of fields) {
			hash.update(field);
		}

		return hash.digest('base64').replace(/[.$|^_]/g, '').slice(0, 12);
	}

	/**
	 * Create hash to use when storing map indexes.
	 *
	 * @method createMapIndexHash
	 * @static
	 * @param {Array{String}} ...fields - An array of fields to use in the hash
	 * @param {Object} options
	 * @return {String} - The map index hash
	 * @since v0.8.0
	 */
	static createMapIndexHash(...fields) {
		let hash = MongoModel.createIndexHash(...fields);
		return `_mapidx_${hash}`;
	}

	/**
	 * Create a field name for use with storing geoHashed indexes.
	 * @method createGeoHashedIndexHash
	 * @static
	 * @param {String} field - The geopoint field being indexed.
	 * @param {Object} step - The polytoken step config for the index. Type is assumed to be 'exponential'.
	 * @return {String} - The indexed field name.
	 */
	static createGeoHashedIndexHash(field, step) {
		let hash = objtools.objectHash({
			field: field,
			step: {
				base: step.base,
				multiplier: step.multiplier,
				stepNum: step.stepNum
			}
		});
		return `_geoidx_${hash}`;
	}

	/**
	 * Add an index to this collection.
	 *
	 * @method index
	 * @chainable
	 * @param {Object} spec - A MongoDb index spec, like: `{ foo: 1, bar: '2dsphere' }`
	 * @param {Object} options - options object
	 * @return {MongoModel} - This model, for chaining
	 * @since v0.0.1
	 */
	index(spec, options = {}) {
		if (this._startedInitializing) {
			throw new XError(XError.INTERNAL_ERROR, 'Cannot add new indexes after initializing');
		}

		if (this.db.options.backgroundIndex) options.background = true;

		// Convert the index type values
		let entry = {
			spec: {},
			options
		};

		let mapSets = {};
		// Convert all paths inside the spec
		for (let path in spec) {
			let { specPath, specIndex, mapPath, field } = this._convertIndexSpec(
				path,
				spec[path],
				this.schema.getSubschemaData(path)
			);

			if (/^_mapidx_/.test(specPath)) {
				// Map index
				if (!mapSets[mapPath]) mapSets[mapPath] = [];
				mapSets[mapPath].push(field);
			} else {
				entry.spec[specPath] = specIndex;
			}
		}

		// Handle any map indexes
		if (!_.isEmpty(mapSets)) {
			if (_.size(mapSets) > 1) {
				// This will cause an NxN index
				throw new MongoError(MongoError.DB_ERROR, 'Cannot index accross maps multiple maps.');
			}
			// The the one mapSets key
			let mapPath = _.keys(mapSets)[0];

			// Concatenate all mapPath/field combinations
			for (let field of mapSets[mapPath].sort()) {
				mapPath += `^${field}`;
			}

			let specPath = MongoModel.createMapIndexHash(mapPath);
			this._indexMapping[specPath] = mapPath;

			entry.spec[specPath] = 1;
			this._addIndexedMapFields(entry);
		}

		this._indexes.push(entry);
		return this;
	}

	/**
	 * Try to find indexed map fields in the given indexes.
	 *
	 * @method _addIndexedMapFields
	 * @private
	 * @param {Object|Object[]} indexes - The index, or indexes to try adding indexed map fields from.
	 * @since v0.5.0
	 */
	_addIndexedMapFields(indexes) {
		if (!_.isArray(indexes)) indexes = [ indexes ];
		for (let index of indexes) {
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
	 * @method _convertIndexSpec
	 * @private
	 * @param {String} path - Field path for this index.
	 * @param {Mixed} value - The value of this index.
	 * @param {Object} subschema
	 * @return {Object} A mongo index object.
	 * @since v0.5.0
	 */
	_convertIndexSpec(path, value, subschema) {
		let result = {};

		let { indexType, indexParams } = MongoModel._convertIndexType(value, subschema);
		result.specIndex = indexType;
		result.indexParams = indexParams;

		// If this is an array path, remove the array index parts
		result.specPath = path.replace(/\.\$/g, '');
		if (this.getSchema().hasParentType(path, 'map')) {
			// If this is a map, we need to make special indexes
			if (result.specIndex !== 1) {
				let msg = 'Cannot index map contents with anything but exact matches';
				throw new XError(XError.INTERNAL_ERROR, msg);
			}
			let parts = result.specPath.split('.');
			result.field = parts.pop();
			result.mapPath = parts.join('|');

			let path = `${result.mapPath}^${result.field}`;

			result.specPath = MongoModel.createMapIndexHash(path);
			this._indexMapping[result.specPath] = path;
		}

		// If this is a geoHashed index, save parameters on the model
		if (indexType === 'geoHashed') {
			if (subschema.type !== 'geopoint') {
				throw new XError(XError.INTERNAL_ERROR, 'geoHashed index must be applied to a geopoint field');
			}
			let stepConfig = result.indexParams && result.indexParams.step;
			if (!stepConfig) {
				throw new XError(XError.INTERNAL_ERROR, 'geoHashed index must have step config');
			}
			if (!stepConfig.base || !stepConfig.multiplier || !stepConfig.stepNum) {
				throw new XError(XError.INTERNAL_ERROR, 'geoHashed step config is missing required parameters');
			}
			stepConfig.type = 'exponential';
			let indexField = MongoModel.createGeoHashedIndexHash(path, stepConfig);
			if (this._geoHashedIndexMapping[result.specPath]) {
				if (this._geoHashedIndexMapping[result.specPath].field !== indexField) {
					throw new XError(
						XError.INTERNAL_ERROR,
						'Cannot place multiple distinct geoHashed indexes on the same field'
					);
				}
			} else {
				this._geoHashedIndexMapping[result.specPath] = {
					field: indexField,
					dimension: new LongLatDimension({ step: stepConfig })
				};
			}
			result = {
				specPath: indexField,
				specIndex: 1
			};
		}

		return result;
	}

	/**
	 * Determines the list of indexes that should be on this collection based on indexes defined in the schema,
	 * and adds them to `this._indexes`.
	 * Each index is represented as an object with two properties:
	 * `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
	 *
	 * @method _fillSchemaIndexes
	 * @private
	 */
	_fillSchemaIndexes() {
		// Crawl the schema to find any indexed single fields
		this.schema.traverseSchema({
			onSubschema: (subschema, specPath/*, subschemaType*/) => {
				if (!_.isObject(subschema) || (!subschema.index && !subschema.unique)) return;

				let specIndex = subschema.index || true;
				if (_.isObject(specIndex)) {
					if (specIndex.indexType) {
						// Index with parameters form
						specIndex = { [specPath]: specIndex };
					} else {
						// Multi-field index form; do nothing
					}
				} else {
					specIndex = { [specPath]: specIndex };
				}

				let isNested = _.includes(specPath, '.');
				let paths = specPath.split('.');
				let basePath = _.initial(paths);
				for (let specKey in specIndex) {
					// If the path is nested, modify the index entry accordingly.
					if (isNested) {
						if (_.includes(specKey, '.')) continue;
						specIndex[`${basePath}.${specKey}`] = specIndex[specKey];
						delete specIndex[specKey];
					}
				}

				if (_.keys(specIndex)[0] !== specPath) {
					throw new XError(XError.INVALID_ARGUMENT, 'Field is not the first index.');
				}

				let options = {};
				if (subschema.sparse) options.sparse = true;
				if (subschema.unique) options.unique = true;
				if (this.db.options.backgroundIndex) options.background = true;
				this.index(specIndex, options);
			}
		}, {
			includePathArrays: true
		});
	}

	/**
	 * Returns all indexes on this model.
	 *
	 * @method getIndexes
	 * @return {Object[]} - Each entry is in the form:
	 *   `{ spec: { age: 1, date: -1 }, options: { sparse: true } }`
	 * @since v0.0.1
	 */
	getIndexes() {
		return this._indexes;
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
			let indexes = this.getIndexes();

			indexes.forEach((index) => {
				let indexKeys = _.keys(index.spec);
				if (indexKeys.length > keys.length) {
					let areSimpleIndexes = _.every(index.spec, (indexValue) => {
						return (indexValue === 1 || indexValue === -1);
					});

					if (areSimpleIndexes) keys = indexKeys;
				}
			});

			keys.reverse();
		}

		// If still no keys, throw an exception
		if (_.isEmpty(keys)) throw new XError(XError.NOT_FOUND, 'No keys can be determined.');

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
	_ensureExists(shouldCreate = true) {
		let db;
		return this.dbPromise
			.then((_db) => {
				db = _db;
				return new Promise((resolve, reject) => {
					db.collection(
						this.getCollectionName(),
						objtools.merge({}, this.collectionOptions, { strict: true }),
						(err, collection) => {
							if (err) return reject(MongoError.fromMongoError(err));

							resolve(collection);
						}
					);
				});
			})
			.catch((err) => {
				if (/does not exist/.test(err.message) && shouldCreate) {
					// Collection doesn't yet exist.  Create it.
					return new Promise((resolve, reject) => {
						db.createCollection(
							this.getCollectionName(),
							objtools.merge({}, this.collectionOptions, { strict: true }),
							(err, collection) => {
								if (err) return reject(MongoError.fromMongoError(err));

								resolve(collection);
							}
						);
					});
				}

				throw err;
			})
			.catch((err) => {
				if (/already exists/.test(err.message) && shouldCreate) {
					return this._ensureExists(false);
				}

				throw err;
			});
	}

	/**
	 * Internal function to create all indexes on a Mongo collection.
	 *
	 * @method _ensureIndexes
	 * @private
	 * @param {mongodb.Collection} collection - The Mongo native driver collection
	 * @return {Promise{mongodb.Collection}} - Resolves with collection or rejects with XError
	 */
	_ensureIndexes(collection) {
		return pasync.eachSeries(this.getIndexes(), (indexInfo) => {
			if (indexInfo.spec._id) {
				return;
			}
			return collection.createIndex(indexInfo.spec, indexInfo.options)
				.then((indexName) => {
					if (indexName && !indexInfo.name) {
						indexInfo.name = indexName;
					}
				})
				.catch((err) => {
					throw MongoError.fromMongoError(err);
				});
		})
			.then(() => {
				return collection.indexes()
					.catch((err) => {
						throw MongoError.fromMongoError(err);
					})
					.then((indexes) => {
						this.indexes = indexes;
						return collection;
					});
			});
	}

	/**
	 * Creates all missing Mongo indexes.
	 *
	 * @method ensureIndexes
	 * @public
	 * @return {Promise{undefined}} - Resolves with undefined or rejects with XError
	 * @since v0.0.1
	 */
	ensureIndexes() {
		let prof = this.profiler.begin('#ensureIndexes');
		return this.collectionPromise
			.then((collection) => this._ensureIndexes(collection))
			.then(() => {})
			.then(prof.wrappedEnd(), prof.wrappedEndError());
	}

	/**
	 * Checks if a Mongo index should exist, according to the schema.
	 *
	 * @method hasIndex
	 * @public
	 * @param {mongodb.Index} index - The Mongo native driver index
	 * @return {Boolean} - True if the index's key matches a spec from the schema, false otherwise.
	 */
	hasIndex(index) {
		return _.some(this.getIndexes(), (schemaIndex) => {
			let sameProperties = _.isEqual(schemaIndex.spec, index.key);
			let sameOrder = _.isEqual(Object.keys(schemaIndex.spec), Object.keys(index.key));

			return sameProperties && sameOrder;
		});
	}

	/**
	 * Internal function to remove all indexes on a Mongo collection that are not in the schema.
	 *
	 * @method _removeIndexes
	 * @private
	 * @param {mongodb.Collection} collection - The Mongo native driver collection
	 * @return {Promise{mongodb.Collection}} - Resolves with collection or rejects with XError
	 */
	_removeIndexes(collection) {
		let convertMongoError = (err) => {
			throw MongoError.fromMongoError(err);
		};

		return collection.indexes()
			.catch(convertMongoError)
			.then((indexes) => {
				return pasync.eachSeries(indexes, (index) => {
					// Drop the index if it is not in the schema, and is not _id
					if (!this.hasIndex(index) && (index.key._id !== 1 || _.keys(index.key).length !== 1)) {
						return collection.dropIndex(index.key)
							.catch(convertMongoError);
					}
				});
			})
			.then(() => {
				return collection.indexes()
					.catch(convertMongoError)
					.then((indexes) => {
						this.indexes = indexes;
						return collection;
					});
			});
	}

	/**
	 * Removes extraneous Mongo indexes.
	 *
	 * @method removeIndexes
	 * @public
	 * @return {Promise{undefined}} - Resolves with undefined or rejects with XError
	 */
	removeIndexes() {
		let prof = this.profiler.begin('#removeIndexes');
		return this.collectionPromise
			.then((collection) => this._removeIndexes(collection))
			.then(() => {})
			.then(prof.wrappedEnd(), prof.wrappedEndError());
	}

	/**
	 * Synchronizes Mongo indexes with those in the schema.
	 *
	 * @method synchronizeIndexes
	 * @public
	 * @return {Promise{undefined}} - Resolves with undefined or rejects with XError
	 */
	synchronizeIndexes() {
		let prof = this.profiler.begin('#synchronizeIndexes');
		return this.removeIndexes()
			.then(() => this.ensureIndexes())
			.then(prof.wrappedEnd(), prof.wrappedEndError());
	}

	_setSchema(schema) {
		if (_.isPlainObject(schema)) {
			schema = createSchema(schema);
		}
		if (!schema.getData().properties._id) {
			this.explicitInternalId = false;
			schema.getData().properties._id = { type: 'mixed' };
		} else {
			this.explicitInternalId = true;
		}
		super._setSchema(schema);
	}

	/**
	 * Loads existing indexes from the mongo db.
	 */
	async _loadMongoIndexes(collection) {
		let r = await collection.indexes();
		for (let rindex of r) {
			let mindex = null;
			for (let ind of this._indexes) {
				if (objtools.deepEquals(ind.spec, rindex.key) && (!ind.name || !rindex.name || ind.name == rindex.name)) {
					mindex = ind;
					break;
				}
			}
			if (!mindex) {
				mindex = {
					name: rindex.name,
					spec: rindex.key,
					options: {
						unique: !!rindex.unique
					}
				};
				this._indexes.push(mindex);
			} else {
				mindex.name = rindex.name;
				if (!mindex.options) mindex.options = {};
				mindex.options.unique = !!rindex.unique;
			}
		}
		this.indexes = r;
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
		if (this._startedInitializing) return this.collectionPromise;

		let prof = this.profiler.begin('#initCollection');

		this._startedInitializing = true;
		this._removeRedundantIndexes();

		return this._ensureExists()
			.then((collection) => {
				// default to automatively create indexes
				if (this.db.options.autoCreateIndex) {
					return this._removeIndexes(collection)
						.then((collection) => this._ensureIndexes(collection));
				}
				return collection;
			})
			.then((collection) => {
				return this._loadMongoIndexes(collection)
					.then(() => collection);
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
			.then(() => this.collectionPromise)
			.then(prof.wrappedEnd(), prof.wrappedEndError());
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
	 * @method _create
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
		let prof = this.profiler.begin('#_createExisting');
		options.isExisting = true;
		let doc = this.create(data, options);
		prof.end();
		return doc;
	}

	/**
	 * Find records in database
	 *
	 * @method find
	 * @param {commonQuery.Query} query - Query for records to find
	 * @param {Object} [options={}] - Mongo options
	 *   @param {String} [options.operationId] - If set, this operation can be
	 *     cancelled later using MongoDb#cancelOperation and the same operation id.
	 *   @param {Number} [options.timeout] - Maximum time for the operation, in seconds.
	 *   @param {Boolean} [options.canCursorTimeout=true] - Whether the cursor may time out.
	 * @return {Array{MongoDocument}} - List of result documents
	 * @since v0.0.1
	 */
	find(query, options = {}) {
		let prof = this.profiler.begin('#find');
		let profMongo;
		let isPartial = !!options.fields;

		let cursor;
		return this.collectionPromise
			.then((collection) => {
				// Transform the query according to the schema
				query = this.normalizeQuery(query);
				profMongo = this.profiler.begin('#find mongo');
				return collection;
			})
			.then((collection) => MongoModel._findWithOptions(collection, query.getData(), this, options))
			.then((mongoCursor) => {
				if (Array.isArray(mongoCursor)) {
					return mongoCursor;
				} else {
					cursor = mongoCursor;
					return cursor.toArray();
				}
			})
			.catch((err) => {
				throw MongoError.fromMongoError(err);
			})
			.then((results) => {
				profMongo.end();
				return _.map(results, (data) => {
					return this._createExisting(data, {
						isPartial,
						fields: options.fields
					});
				});
			})
			.then((results) => {
				if (options.total) {
					return cursor.count()
						.then((total) => {
							results.total = total;
							return results;
						});
				}

				return results;
			})
			.then(prof.wrappedEnd(), prof.wrappedEndError());
	}

	/**
	 * Find records in database
	 *
	 * @method findStream
	 * @param {commonQuery.Query} query - Query for records to find
	 * @param {Object} [options={}] - Mongo options
	 *   @param {String} [options.operationId] - If set, this operation can be
	 *     cancelled later using MongoDb#cancelOperation and the same operation id.
	 * @return {CursorResultStream} - List of result documents
	 * @since v0.0.1
	 */
	findStream(query, options = {}) {
		let prof = this.profiler.begin('#findStream');
		let isPartial = !!options.fields;

		// Run the query, streaming the results
		let resultStream = new PassThrough({ objectMode: true });
		let stream = new CursorResultStream(this, null, { isPartial });
		this.collectionPromise
			.then((collection) => {
				// Transform the query according to the schema
				query = this.normalizeQuery(query);
				return collection;
			})
			.then((collection) => {
				return MongoModel._findWithOptions(collection, query.getData(), this, options);
			})
			.then((cursor) => {
				if (Array.isArray(cursor)) {
					for (let data of cursor) {
						let doc = this._createExisting(data, {
							isPartial,
							fields: options.fields
						});
						resultStream.write(doc);
					}
				} else {
					stream.pipe(resultStream);
					stream.setCursor(cursor);
				}
			})
			.catch((err) => {
				resultStream.emit('error', err);
			})
			.then(prof.wrappedEnd(), prof.wrappedEndError())
			.catch(pasync.abort);
		return resultStream;
	}

	/**
	 * Insert record into the database
	 *
	 * @method insert
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
		let prof = this.profiler.begin('#insertMulti');

		// Transform the mongo options
		let mongoOptions = MongoModel._transformMongoOptions(options, this.db);

		options.serialize = true;

		// Normalize the data according to the schema
		for (let data of datas) {
			this.schema.normalize(data, options);
			this.normalizeDocumentIndexedMapValues(data);
			this.normalizeDocumentIndexedGeoHashedValues(data);
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
			})
			.then(prof.wrappedEnd(), prof.wrappedEndError());
	}

	/**
	 * Count records in database
	 *
	 * @method count
	 * @param {commonQuery.Query} query - Query for record(s) to remove
	 * @param {Object} [options={}] - Mongo options
	 *   @param {String} [options.operationId] - If set, this operation can be
	 *     cancelled later using MongoDb#cancelOperation and the same operation id.
	 *   @param {Number} [options.timeout] - Maximum time for the operation, in seconds.
	 * @return {Number} - The number of matched records
	 * @since v0.0.1
	 */
	count(query, options = {}) {
		let prof = this.profiler.begin('#count');

		// Transform the mongo options
		let mongoOptions = MongoModel._transformMongoOptions(options, this.db);

		return this.collectionPromise
			.then((collection) => {
				// Transform the query according to the schema
				query = this.normalizeQuery(query);
				return collection;
			})
			.then((collection) => {
				// Add operationId as comment, if any
				let queryData = opUtils.addComment(query.getData(), options.operationId);
				let cursor = collection.find(queryData, mongoOptions);
				if (_.isNumber(options.timeout)) {
					cursor = cursor.maxTimeMS(MongoModel._convertTimeout(options.timeout));
				}

				return cursor.count();
			})
			.catch((err) => {
				throw MongoError.fromMongoError(err);
			})
			.then(prof.wrappedEnd(), prof.wrappedEndError());
	}

	/**
	 * Check whether the running mongo instance has support for facets.
	 *
	 * @method _hasFacetSupport
	 * @private
	 * @return {Boolean}
	 */
	_hasFacetSupport() {
		const { versionArray } = this.db.serverInfo;
		return versionArray[0] >= 3 && versionArray[1] >= 4;
	}

	/**
	 * Perform multiple aggregates on the database
	 *
	 * @method aggregateMulti
	 * @param {commonQuery.Query} query - Query for records on which to perform the aggregates
	 * @param {Object{Object}|Object{commonQuery.Aggregate}} aggregates - Table of aggregate queries,
	 *   where the key is the user-defined name of the aggregate, and the value is a commonQuery aggregate
	 * @param {Object} [options={}] - Mongo options
	 *   @param {String} [options.operationId] - If set, this operation can be
	 *     cancelled later using MongoDb#cancelOperation and the same operation id.
	 *   @param {Number} [options.scanLimit] - Maximum number of results passing the filter stage that
	 *     are scanned to aggregate.
	 *   @param {Number} [options.timeout] - Maximum time for the operation, in seconds.
	 *   @param {Boolean} [options.canCursorTimeout=true] - Whether the cursor may time out.
	 * @return {Promise{Array{Object}}} - Resolves to table of aggregate results, in the commonQuery syntax
	 * @since v0.1.0
	 */
	aggregateMulti(query, aggregates, options = {}) {
		let prof = this.profiler.begin('#aggregateMulti');
		const useFacet = this._hasFacetSupport();
		options = objtools.merge({ useFacet }, options);

		let pipelines;
		return this.collectionPromise
			.then((collection) => {
				// Transform the query and aggregates according to the schema
				query = this.normalizeQuery(query);
				for (let key in aggregates) {
					aggregates[key] = this.normalizeAggregate(aggregates[key]);
				}

				pipelines = aggregateUtils.createAggregatePipelines(this.schema, query, aggregates, options);
				return collection;
			})
			.then((collection) => {
				let results = pipelines.map((pipelineData) => {
					if (_.isNumber(options.timeout)) {
						// Replace timeout option with mongo maxTimeMS option.
						// This option is not documented on aggregates, but appears to work.
						options.maxTimeMS = MongoModel._convertTimeout(options.timeout);
						delete options.timeout;
					}

					let cursor = collection.aggregate(pipelineData.pipeline, options);
					//if (options.canCursorTimeout === false) cursor.addCursorFlag('noCursorTimeout', true);

					return cursor.toArray()
						.catch((err) => { throw MongoError.fromMongoError(err); })
						.then((results) => {
							pipelineData.results = results;
							return pipelineData;
						});
				});

				return Promise.all(results)
					.then((pipelines) => {
						return aggregateUtils.createAggregateResult(this.schema, pipelines, aggregates, useFacet);
					});
			})
			.then((results) => {
				// ensure all keys in the aggregate are represented in the result
				for (let key in aggregates) {
					if (results[key] === undefined) {
						let aggregateData = aggregates[key].getData();
						if (aggregateData.groupBy !== undefined) {
							results[key] = [];
						} else {
							results[key] = aggregateData.total ? { total: 0 } : {};
						}
					}
				}
				return results;
			})
			.then(prof.wrappedEnd(), prof.wrappedEndError());
	}

	/**
	 * Remove records from database
	 *
	 * @method remove
	 * @param {commonQuery.Query} query - Query for records to remove
	 * @param {Object} [options={}] - Mongo options
	 *   @param {Boolean} [options.forceResave=false] - Causes the removal to
	 *     be performed by requesting documents and calling `#remove` on each.
	 * @return {Object} - The response from the mongo command
	 * @since v0.0.1
	 */
	remove(query, options = {}) {
		let prof = this.profiler.begin('#remove');

		// Transform the mongo options
		let mongoOptions = MongoModel._transformMongoOptions(options, this.db);

		if (!options.forceResave) {
			// Perform removal normally.
			return this.collectionPromise
				.then((collection) => {
					// Transform the query according to the schema
					query = this.normalizeQuery(query);
					return collection;
				})
				.then((collection) => collection.deleteMany(
					query.getData(),
					mongoOptions
				))
				.then(prof.wrappedEnd(), prof.wrappedEndError());
		} else {
			// Remove each matched document by calling #remove.
			let numRemoved = 0;
			return this.findStream(query, mongoOptions)
				.each((doc) => {
					return doc.remove()
						.then(() => {
							numRemoved += 1;
						});
				})
				.intoPromise()
				.then(() => numRemoved)
				.then(prof.wrappedEnd(), prof.wrappedEndError());
		}
	}

	/**
	 * Update records in database
	 *
	 * @method update
	 * @param {commonQuery.Query} query - Query for records to update
	 * @param {commonQuery.Update} update - Update query
	 * @param {Object} [options={}] - Mongo options
	 *   @param {Boolean} [options.returnDocument=false] - If true, the first updated or inserted
	 *     document is returned as a MongoDocument.
	 *   @param {Boolean} [options.forceAtomic=false] - Force atomic updates even if updating indexed map fields.
	 *     This will result in inconsistent map index data.
	 *   @param {Boolean} [options.forceResave=false] - Force update through document saves, even if not
	 *     updating indexed map fields.
	 * @return {Promise} - Resolves with the number of documents updated, or rejects with XError
	 * @since v0.0.1
	 */
	async update(query, update, options = {}) {
		return await this.profiler.run('#update', async() => {

			// Transform the mongo options
			let returnDocument = options.returnDocument;
			delete options.returnDocument;
			let mongoOptions = MongoModel._transformMongoOptions(options, this.db);
			let updateOptions = _.pick(options, [ 'skipFields' ]);

			// Transform the query and update according to the schema
			update = this.normalizeUpdate(update, _.assign({ serialize: true, schema: this.schema }, options));

			// Set multi option if and only if this is not a full replace.
			if (update.hasOperators()) {
				mongoOptions.multi = true;
			}

			// Check if this is accessing a field inside a map
			let updateFields = update.getUpdatedFields();
			let isUpdatingMap = false, isUpdatingGeoHashed = false;
			for (let field of updateFields) {
				if (this.getSchema().hasParentType(field, 'map')) {
					isUpdatingMap = true;
					break;
				}
				for (let geoHashedField in this._geoHashedIndexMapping) {
					if (geoHashedField.startsWith(field)) {
						isUpdatingGeoHashed = true;
						break;
					}
				}
				if (isUpdatingGeoHashed) break;
			}

			// Determine whether or not this update should be performed atomically.
			let isAtomic = (!isUpdatingMap && !isUpdatingGeoHashed);
			isAtomic = options.forceAtomic || isAtomic;
			isAtomic = !options.forceResave && isAtomic;

			let updateFn = update.isFullReplace() ? 'replaceOne' : 'updateOne';

			if (isAtomic) {
				// We can run a normal update on this, since it's not touching map or geoHashed data
				let collection = await this.collectionPromise;
				query = this.normalizeQuery(query);
				let result;
				let numRetries = 3;
				while (numRetries > 0) {
					try {
						result = await collection[updateFn](query.getData(), update.getData(), mongoOptions);
						break;
					} catch (err) {
						err = MongoError.fromMongoError(err, this);
						if (err.code === XError.ALREADY_EXISTS && options.upsert && numRetries > 1) {
							// Upserts in mongo aren't actually atomic.  There are cases where two parallel processes
							// can both try to insert the document at the same time, and one will fail.  The recommended
							// solution is to retry in this case.
							numRetries--;
							continue;
						} else {
							throw err;
						}
					}
				}

				if (returnDocument) {
					let upsertedId = objtools.getPath(result, 'result.upserted.0._id');
					if (upsertedId) {
						try {
							return this.findOne({ _id: upsertedId }, mongoOptions);
						} catch (err) {
							if (err.code === XError.NOT_FOUND) {
								// This should never happen
								return undefined;
							} else {
								throw MongoError.fromMongoError(err, this);
							}
						}
					} else {
						return undefined;
					}
				} else {
					return result.result.nModified;
				}
			} else {
				// We need to rebuild map data, so run this as a streaming, in memory save
				let numUpdated = 0;
				let firstSavedDoc;
				await this.findStream(query, mongoOptions).each(async(doc) => {
					if (!firstSavedDoc) firstSavedDoc = doc;
					update.apply(doc.getData(), updateOptions);
					await doc.save();
					numUpdated++;
				}).intoPromise();

				// 'Upsert' the document if that flag is set and we didn't find any document
				if (options.upsert && numUpdated === 0) {
					let newDocData = {};
					update.apply(newDocData, updateOptions);
					let newDoc = this.create(newDocData);
					firstSavedDoc = newDoc;
					await newDoc.save();
					numUpdated++;
				}
				if (returnDocument) {
					return firstSavedDoc;
				} else {
					return numUpdated;
				}
			}
		});
	}

	/**
	 * Updates all documents matching a given query if they exist, and otherwise creates one.
	 *
	 * @method upsert
	 * @param {commonQuery.Query} query - Query for records to update
	 * @param {commonQuery.Update} update - Update query
	 * @param {Object} [options={}] - Mongo options
	 * @return {Promise} - Resolves with first updated/inserted document
	 * @since v0.5.0
	 */
	upsert(query, update, options = {}) {
		options.upsert = true;
		options.returnDocument = true;
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

		// If no mapFields, then there's nothing to try to optimize
		if (mapFields.length === 0) return;

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
					if (!_.includes(validOperators, operator)) {
						foundInvalid = true;
						break;
					}
				}

				// Found an invalid operator, just do table scan
				if (foundInvalid) return;
			}
		}

		// Get contracted map path (with keys extracted, and the start of the specPath for the map)
		let { specPath, keys } = this._getContractedMapPath(mapPath);
		if (!keys.length) return;

		// Append fields we want to the specPath
		for (let mapField of mapFields) {
			specPath += `^${mapField}`;
		}

		let hash = MongoModel.createMapIndexHash(specPath);
		this._indexMapping[hash] = specPath;

		specPath = hash;

		// Ensure the map we're lookg for is actually indexed
		let foundIndex = false;
		for (let index of this.getIndexes()) {
			let indexSpecPaths = _.keys(index.spec);

			if (indexSpecPaths.length !== 1) continue;

			if (indexSpecPaths[0] === specPath) {
				foundIndex = true;
				break;
			}
		}

		// This map path isn't indexed
		if (!foundIndex) return;

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
					hiVal = bson.serialize(keys.concat([ fieldValue[hiOp] ]));
				}
				if (fieldValue.$gt || fieldValue.$gte) {
					loOp = (fieldValue.$gte) ? '$gte' : '$gt';
					loVal = bson.serialize(keys.concat([ fieldValue[loOp] ]));
				}

				// Something weird is happening. Bomb out!
				if (loOp === undefined && hiOp === undefined) return;

				if (loOp === undefined) {
					loOp = '$gte';
					loVal = bson.serialize(keys);
					loVal[0] = hiVal[0];
				} else if (hiOp === undefined) {
					hiOp = '$lt';
					hiVal = bson.serialize(keys);
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
				data[specPath] = bson.serialize(keys).toString();

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
			data[specPath] = bson.serialize(keys).toString();
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
		specPath = specPath.replace('.', '|');

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
	 * Normalize the data inside indexed map fields,
	 * which get converted to arrays of stringified BSON data.
	 *
	 * @method normalizeDocumentIndexedMapValues
	 * @param {Object} data - The raw docuemnt data to normalize.
	 * @since v0.5.0
	 */
	normalizeDocumentIndexedMapValues(data) {
		let indexedMapFields = this.getIndexedMapFields();

		for (let indexedMapField of indexedMapFields) {
			let mapField = this._indexMapping[indexedMapField];

			// Split on field identifier
			let [ mapPath, ...mapFields ] = mapField.split('^');
			let components = mapPath.split('|');
			let expandedPaths = this._getExpandedMapPaths(data, '', components);

			for (let { expandedPath, keys } of expandedPaths) {
				let indexData = objtools.deepCopy(keys);
				let allFound = true;

				// TODO: support sparse indexes, and non-field indexes
				for (let field of mapFields) {
					let fieldPath = (expandedPath ? `${expandedPath}.` : '') + field;
					let value = objtools.getPath(data, fieldPath);
					if (_.isUndefined(value) || _.isNull(value)) {
						allFound = false;
						break;
					}
					indexData.push(value);
				}

				if (allFound) {
					// Add the bson indexed field to data
					if (!data[indexedMapField]) { data[indexedMapField] = []; }
					data[indexedMapField].push(bson.serialize(indexData).toString());
				}
			}
		}
	}

	/**
	 * Generate polytoken take for geoHashed indexed geopoints.
	 *
	 * @method normalizeDocumentIndexedGeoHashedValues
	 * @param {Object} data - The raw docuemnt data to normalize.
	 * @since v0.5.0
	 */
	normalizeDocumentIndexedGeoHashedValues(data) {
		if (Object.keys(this._geoHashedIndexMapping).length === 0) return;
		let polytokenSets = {};  // Map of index field to set of polytokens for that field
		this.schema.traverse(data, {
			onField: (field, value, subschema) => {
				if (!value || subschema.type !== 'geopoint') return;
				// Remove array indices from field path
				let transformedField = field.replace(/\.[0-9]+/g, '');
				if (this._geoHashedIndexMapping[transformedField]) {
					let geoHashInfo = this._geoHashedIndexMapping[transformedField];
					let polytokens = geoHashInfo.dimension.getTokensForPoint(value);
					if (!polytokenSets[geoHashInfo.field]) polytokenSets[geoHashInfo.field] = {};
					for (let polytoken of polytokens) {
						polytokenSets[geoHashInfo.field][polytoken] = true;
					}
				}
			}
		});
		for (let key in polytokenSets) {
			data[key] = Object.keys(polytokenSets[key]);
		}
	}

	/**
	 * Indexes that are a strict prefix of another index are redundant.
	 * An index is a strict prefix if all of its keys are the same n keys of another index,
	 * in the same order, with equal values at each key, and deeply equal options.
	 *
	 * @method _removeRedundantIndexes
	 * @returns [Object] - returns the new index set
	 */
	_removeRedundantIndexes() {
		const isDuplicateIndex = (potentialDuplicateIndex) => {
			let {
				options: potentialDuplicateOptions,
				spec: potentialDuplicateSpec
			} = potentialDuplicateIndex;
			for (let index of this._indexes) {
				let { options, spec } = index;
				if (
					potentialDuplicateIndex !== index &&
					_.isEqual(options, potentialDuplicateOptions) &&
					isPrefix(spec, potentialDuplicateSpec)
				) {
					return true;
				}
			}
			return false;
		};
		this._indexes = _.reject(this._indexes, isDuplicateIndex);
		return this._indexes;
	}
}

// helper function for #_removeRedundantIndexes
function isPrefix(map, subMap) {
	let keys = _.keys(map);
	for (let key in subMap) {
		if (key !== keys.shift() || subMap[key] !== map[key]) return false;
	}
	return true;
}

module.exports = MongoModel;
