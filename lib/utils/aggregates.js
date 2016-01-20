const _ = require('lodash');
const moment = require('moment');
const XError = require('xerror');
const objtools = require('zs-objtools');

const multiFieldValueSeparator = ' |-| ';

/**
 * Construct an expression that converts the given expression result into a string
 *
 * @method toStringExpr
 * @param {String|Object} expr - A MongoDB field expression
 * @return {Object} - A MongoDB aggregate expression
 * @since v0.1.0
 */
const toStringExpr = (expr) => {
	return { $substr: [ expr, 0, 256 ] };
};

/**
 * Construct an expression that gives the numeric value of the start of the range, given a base expression
 *
 * @method toIntervalExpr
 * @param {String|Object} expr - A MongoDB field expression
 * @param {Number} [interval=1] - Interval to group aggregate results by
 * @param {Number} [base=0] - This is the starting value of the first interval in the sequence
 * @param {Number} [offset=0] - Static value to add to the interval number of the expression result.
 *   This is implemented as simply adding this value to the interval number.
 * @return {Object} - A MongoDB aggregate expression
 * @since v0.1.0
 */
const toIntervalExpr = (expr, interval = 1, base = 0, offset = 0) => {
	if (interval === 1) return expr;

	if (base !== 0) {
		expr = { $subtract: [ expr, base ] };
	}

	/*
	 * MongoDB does not work correctly with negative moduli.
	 * This transforms the input expression to something equivalent that works in MongoDB.
	 * It works by conditionally subtracting `interval - 1` from input values,
	 * which corrects an offset that affects negative input values.
	 */
	let fixedExpr = {
		$cond: {
			if: { $lt: [ expr, 0 ] },
			then: { $subtract: [ expr, interval - 1 ] },
			else: expr
		}
	};

	expr = { $subtract: [
		'$$fixedExpr',
		{ $mod: [ '$$fixedExpr', interval ] }
	] };

	if (base !== 0 || offset !== 0) {
		expr = { $add: [ expr, base + offset ] };
	}

	return {
		$let: {
			vars: { fixedExpr },
			in: expr
		}
	};
};

/**
 * Set all unset time components to their base value
 *
 * @method fillRemainingTimeComponents
 * @param {String|Object} ...components - MongoDB expressions which are components of an ISO 8601 time string
 * @return {Array} - An array of ISO 8601 time components, each of which can be a String or MongoDB expression
 * @since v0.1.0
 */
const fillRemainingTimeComponents = (...components) => {
	if (components.length < 1) components.push('1970');
	if (components.length < 2) components.push('-');
	if (components.length < 3) components.push('01');
	if (components.length < 4) components.push('-');
	if (components.length < 5) components.push('01');
	if (components.length < 6) components.push('T');
	if (components.length < 7) components.push('00');
	if (components.length < 8) components.push(':');
	if (components.length < 9) components.push('00');
	if (components.length < 10) components.push(':');
	if (components.length < 11) components.push('00');
	if (components.length < 12) components.push('Z');

	return components;
};

/**
 * Sort aggregate results by comparison of keys
 *
 * @method keySort
 * @param {Object} a
 * @param {Object} b
 * @return {Number} - Sort comparison result
 * @since v0.1.0
 */
const keySort = (a, b) => {
	for (let key in a.key) {
		if (''+a.key[key] > ''+b.key[key] || typeof b.key[key] === 'undefined') return 1;
		if (''+a.key[key] < ''+b.key[key]) return -1;
	}
	return 0;
};

/**
 * Take a time interval ISO 8601 string part and formats it correctly
 *
 * @example:
 *   formatTime('2014-9-12T4') // -> '2014-09-12T04'
 *
 * @method formatTime
 * @param {String} value - Potentially non-padded value which is otherwise a valid ISO 8601 time part
 * @return {String} - Properly-formatted ISO 8601 string part
 * @since v0.1.0
 */
const formatTime = (value) => {
	let components = value.split('T');
	let result = '';

	if (components[0]) {
		let dayComponents = components[0].split('-');
		if (dayComponents[0]) result += _.padLeft(dayComponents[0], 4, '0');
		if (dayComponents[1]) result += '-' + _.padLeft(dayComponents[1], 2, '0');
		if (dayComponents[2]) result += '-' + _.padLeft(dayComponents[2], 2, '0');
	}

	if (components[1]) {
		let timeComponents = components[1].split(':');
		if (timeComponents[0]) result += 'T' + _.padLeft(timeComponents[0], 2, '0');
		if (timeComponents[1]) result += ':' + _.padLeft(timeComponents[1], 2, '0');
		if (timeComponents[2]) result += ':' + _.padLeft(timeComponents[2], 2, '0');
	}

	return result;
};
/**
 * Returns a time interval expression,
 * where output values are ISO 8601 timestamp partials denoting the beginning of each range.
 *
 * @method makeTimeComponentExpression
 * @param {Schema} schema
 * @param {String|Object} field - A MongoDB expression, to be transformed into a time interval
 * @param {String} unit - The unit of the time interval
 *   Possible values: year, month, day, hour, minute, second
 * @param {Number} interval - The number of units to be included in each time interval
 * @return {Object} - A MongoDB aggregate expression
 * @since v0.1.0
 */
exports.makeTimeComponentExpression = function(schema, field, unit, interval = 1) {
	let $field = '$' + field;

	if (typeof unit !== 'string') {
		throw new XError(XError.INVALID_ARGUMENT, 'Aggregate time unit must be string', { unit });
	}

	let components;

	switch (unit) {
			case 'year':
				components = fillRemainingTimeComponents(
					toStringExpr(toIntervalExpr({ $year: $field }, interval))
				);
				break;
			case 'month':
				components = fillRemainingTimeComponents(
					toStringExpr({ $year: $field }), '-',
					toStringExpr(toIntervalExpr({ $month: $field }, interval, 1))
				);
				break;
			case 'day':
				components = fillRemainingTimeComponents(
					toStringExpr({ $year: $field }), '-',
					toStringExpr({ $month: $field }), '-',
					toStringExpr(toIntervalExpr({ $dayOfMonth: $field }, interval, 1))
				);
				break;
			case 'hour':
				components = fillRemainingTimeComponents(
					toStringExpr({ $year: $field }), '-',
					toStringExpr({ $month: $field }), '-',
					toStringExpr({ $dayOfMonth: $field }), 'T',
					toStringExpr(toIntervalExpr({ $hour: $field }, interval))
				);
				break;
			case 'minute':
				components = fillRemainingTimeComponents(
					toStringExpr({ $year: $field }), '-',
					toStringExpr({ $month: $field }), '-',
					toStringExpr({ $dayOfMonth: $field }), 'T',
					toStringExpr({ $hour: $field }), ':',
					toStringExpr(toIntervalExpr({ $minute: $field }, interval))
				);
				break;
			case 'second':
				components = fillRemainingTimeComponents(
					toStringExpr({ $year: $field }), '-',
					toStringExpr({ $month: $field }), '-',
					toStringExpr({ $dayOfMonth: $field }), 'T',
					toStringExpr({ $hour: $field }), ':',
					toStringExpr({ $minute: $field }), ':',
					toStringExpr(toIntervalExpr({ $second: $field }, interval))
				);
				break;
			default:
				throw new XError(XError.INVALID_ARGUMENT, 'Invalid aggregate time unit', { unit });
	}

	return {
		$concat: components
	};
};

/**
 * Given a numeric interval,
 * returns an expression that will result in the start value of the interval in which the field given lies.
 *
 * @method makeIntervalExpression
 * @param {Schema} schema
 * @param {String|Object} field - A MongoDB expression, to be transformed into a numeric interval
 * @param {Number} interval - The number of units to be included in each time interval
 * @return {Object} - A MongoDB aggregate expression
 * @since v0.1.0
 */
exports.makeIntervalExpression = function(schema, field, interval, base = 0) {
	if (typeof interval === 'string' && interval[0] === 'P') {
		interval = moment.duration(interval);

		let subschema = schema.getSubschemaData(field);
		let fieldType = subschema.type;
		if (fieldType === 'array') {
			fieldType = subschema.elements.type;
		}
		if (fieldType === 'date') {
			throw new XError(
				XError.UNSUPPORTED_OPERATION,
				`Intervals are not currently supported on ${fieldType} fields.`,
				{ field }
			);
		}
	}

	interval = +interval;

	if (_.isNaN(interval)) {
		throw new XError(XError.INVALID_ARGUMENT, 'Aggregate interval must be numeric.', { interval });
	}

	return toIntervalExpr(`$${field}`, interval, base);
};

/**
 * Given a set of ranges, makes an expression that will map values in each of the ranges to that range's index.
 *
 * @method makeRangesExpression
 * @param {Schema} schema
 * @param {String|Object} field - A MongoDB expression, to be transformed into a range interval
 * @param {Array{*}} ranges - A list of commonQuery aggregate ranges
 * @return {Object} - A MongoDB aggregate expression
 * @since v0.1.0
 */
exports.makeRangesExpression = function(schema, field, ranges) {
	if (!Array.isArray(ranges)) throw new XError(XError.INVALID_ARGUMENT, 'Aggregate ranges must be array', { ranges });

	let curExpr = { $literal: -1 };

	ranges.forEach((range, rangeIdx) => {
		let boundsExpr = [];

		if (range.start !== undefined && range.start !== null) {
			boundsExpr.push({ $gte: [ '$' + field, range.start || 0 ] });
		}

		if (range.end !== undefined && range.end !== null) {
			boundsExpr.push({ $lt: [ '$' + field, range.end || 0 ] });
		}

		if (boundsExpr.length === 1) {
			boundsExpr = boundsExpr[0];
		} else {
			boundsExpr = { $and: boundsExpr };
		}

		curExpr = {
			$cond: [
				boundsExpr,
				rangeIdx,
				curExpr
			]
		};
	});

	return curExpr;
};

/**
 * Function to return a $group expression for each aggregate type
 *
 * @method makeGroupExpression
 * @param {Schema} schema
 * @param {commonQuery.Aggregate} aggregate
 * @return {Object}
 * @since v0.1.0
 */
exports.makeGroupExpression = function(schema, aggregate) {
	let aggregateData = aggregate.getData();

	if (!aggregateData.groupBy) {
		if (!aggregateData.stats) {
			throw new XError(XError.UNSUPPORTED_OPERATION, 'Aggregate type is not supported.');
		}

		return {
			idExpr: null,
			fields: []
		};
	}

	// Keep track of which fields are actually arrays, so we know to introduce the
	// $unwind pipeline stages for them later.
	let unwindFieldSet = {};

	let groupBy = aggregateData.groupBy;
	if (!_.isArray(groupBy)) groupBy = [ groupBy ];
	// Construct an array of aggregate expressions that generate the value to group by for each component
	let groupValueSpecs = groupBy.map((groupBySpec) => {
		if (typeof groupBySpec === 'string') {
			groupBySpec = { field: groupBySpec };
		}

		if (
			!groupBySpec ||
			typeof groupBySpec !== 'object' ||
			!groupBySpec.field ||
			typeof groupBySpec.field !== 'string'
		) {
			throw new XError(
				XError.INVALID_ARGUMENT,
				'A groupBy spec must contain a string "field" parameter',
				{ groupBy, groupBySpec }
			);
		}

		let valueExpr;
		let field = groupBySpec.field;

		if (schema.getSubschemaData(field).type === 'array') {
			unwindFieldSet[field] = true;
		}

		if (groupBySpec.ranges) {
			valueExpr = this.makeRangesExpression(schema, field, groupBySpec.ranges);
		} else if (groupBySpec.interval) {
			valueExpr = this.makeIntervalExpression(schema, field, groupBySpec.interval, groupBySpec.base);
		} else if (groupBySpec.timeComponent) {
			valueExpr = this.makeTimeComponentExpression(
				schema,
				field,
				groupBySpec.timeComponent,
				groupBySpec.timeComponentCount
			);
		} else {
			valueExpr = '$' + field;
		}

		return {
			field,
			valueExpr
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
		fields: groupValueSpecs,
		unwindFields: _.keys(unwindFieldSet)
	};
};

/**
 * Create aggregate request, converting our own syntax to MongoDB's
 *
 * @method createAggregatePipelines
 * @param {Schema} schema
 * @param {commonQuery.Query} query - Query for records on which to perform the aggregates
 * @param {Array{commonQuery.Aggregate}} aggregates - Table of aggregate queries
 * @return {Array{Object}} - Array of MongoDB aggregate pipelines
 * @since v0.1.0
 */
exports.createAggregatePipelines = function(schema, query, aggregates) {
	let aggregatesByGroup = {};
	let queryData = query.getData();

	_.forEach(aggregates, (aggregate, index) => {
		let groupExpression = this.makeGroupExpression(schema, aggregate);
		let groupHash = objtools.objectHash(groupExpression.idExpr);

		if (aggregatesByGroup[groupHash]) {
			aggregatesByGroup[groupHash].specIndices.push(index);
		} else {
			aggregatesByGroup[groupHash] = {
				groupExpression,
				specIndices: [ index ]
			};
		}
	});

	// Maintain a mapping between field names and numbers
	let fieldNumMap = {};
	let fieldCounter = 1;

	// For each group expression, construct a set of fields to return
	_.forEach(aggregatesByGroup, (groupExpressionData) => {
		let fields = {};

		groupExpressionData.specIndices.forEach((index) => {
			let aggregateSpec = aggregates[index].getData();

			// Add field passthroughs
			groupExpressionData.groupExpression.fields.forEach((fieldExprSpec, fieldIdx) => {
				fields[`agg_${index}_key_${fieldIdx}`] = { $first: fieldExprSpec.valueExpr };
			});

			// Add aggregate fields to the pipeline
			if (aggregateSpec.stats) {
				_.forEach(aggregateSpec.stats, (stat, field) => {
					let fieldNum;
					if (fieldNumMap[field]) {
						fieldNum = fieldNumMap[field];
					} else {
						fieldNum = fieldNumMap[field] = fieldCounter++;
					}
					if (stat.count) {
						fields[`agg_${index}_count_${fieldNum}`] = {
							$sum: {
								$cond: [
									{ $eq: [
										{ $ifNull: [ `$${field}`, null ] },
										null
									] },
									0,
									1
								]
							}
						};
					}
					if (stat.avg) fields[`agg_${index}_avg_${fieldNum}`] = { $avg: `$${field}` };
					if (stat.min) fields[`agg_${index}_min_${fieldNum}`] = { $min: `$${field}` };
					if (stat.max) fields[`agg_${index}_max_${fieldNum}`] = { $max: `$${field}` };
				});
			}

			if (aggregateSpec.total) fields[`agg_${index}_total`] = { $sum: 1 };
		});

		groupExpressionData.fields = fields;
	});

	// Construct an aggregate pipeline for each group by expression
	let pipelines = [];
	_.forEach(aggregatesByGroup, (groupExpressionData) => {
		groupExpressionData.fields._id = groupExpressionData.groupExpression.idExpr;

		let unwindFields = groupExpressionData.groupExpression.unwindFields || [];
		let unwindStages = unwindFields.map((arrayField) => {
			return { $unwind: '$' + arrayField };
		});

		let pipeline = [];
		pipeline.push({ $match: queryData });
		pipeline.push(...unwindStages);
		pipeline.push({ $group: groupExpressionData.fields });
		groupExpressionData.pipeline = pipeline;
		groupExpressionData.fieldNumMap = fieldNumMap;

		pipelines.push(groupExpressionData);
	});

	return pipelines;
};

/**
 * Create aggregate result, converting MongoDB's result to our own
 *
 * @method createAggregateResult
 * @param {Schema} schema
 * @param {Array{Object}} pipelines - MongoDB aggregate results
 * @param {Array{commonQuery.Aggregate}} aggregates - Table of aggregate queries
 * @return {Object} - Table of aggregate results, in the commonQuery syntax
 * @since v0.1.0
 */
exports.createAggregateResult = function(schema, pipelines, aggregates) {
	let aggregateResults = {};

	for (let pipelineData of pipelines) {
		let { results, fieldNumMap } = pipelineData;
		let numFieldMap = _.invert(fieldNumMap);
		let keyPattern = /^agg_([A-Za-z0-9-]+)_([A-Za-z0-9-]+)(?:_(.*))?$/;

		for (let resultEntryIdx = 0; resultEntryIdx < results.length; resultEntryIdx++) {
			let resultEntry = results[resultEntryIdx];
			let resultsByAggregate = {};

			// Extract the fields for each aggregate from the pipeline result
			for (let resultKey in resultEntry) {
				let matches = keyPattern.exec(resultKey);
				if (matches) {
					let [ matchGroup, matchOperator, matchFieldNum ] = _.tail(matches);

					if (matchOperator === 'key') {
						matchOperator = `${matchOperator}_${matchFieldNum}`;
						matchFieldNum = null;
					}

					if (!resultsByAggregate[matchGroup]) {
						resultsByAggregate[matchGroup] = { stats: {} };
					}

					if (matchFieldNum) {
						let matchField = numFieldMap[matchFieldNum];
						if (!matchField) throw new XError(XError.INTERNAL_ERROR, 'Unexpected field in result');
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
			for (let aggregateName in resultsByAggregate) {
				let result = resultsByAggregate[aggregateName];
				let aggrSpec = aggregates[aggregateName].getData();
				let groupBy = aggrSpec.groupBy;

				// Delete stats if empty
				if (_.isEmpty(result.stats)) delete result.stats;

				if (groupBy) {
					if (!_.isArray(groupBy)) groupBy = [ groupBy ];

					let keyArray = [];

					for (let keyIndex = 0; keyIndex < groupBy.length; keyIndex++) {
						let keyValue = result[`key_${keyIndex}`];
						delete result[`key_${keyIndex}`];

						if (keyValue === undefined) keyValue = null;

						if (groupBy[keyIndex].timeComponent) keyValue = formatTime(keyValue);

						keyArray.push(keyValue);
					}

					result.key = keyArray;

					if (!aggregateResults[aggregateName]) aggregateResults[aggregateName] = [];
					aggregateResults[aggregateName].push(result);
				} else {
					aggregateResults[aggregateName] = result;
				}
			}
		}
	}

	// Sort results by string representation of the keys
	for (let aggregateName in aggregateResults) {
		if (_.isArray(aggregateResults[aggregateName])) aggregateResults[aggregateName].sort(keySort);
	}

	return aggregateResults;
};
