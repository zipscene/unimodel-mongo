const objtools = require('objtools');
const _ = require('lodash');

/**
 * Utility functions for working with results of the mongo currentOp command.
 *
 * @class ops
 * @private
 * @static
 */

/**
 * Returns a copy of the provided query object with a top-level comment added
 * as the first key. This is necessary to ensure that the comment will appear
 * even in pre-mongo-3.5 truncated queries.
 *
 * @method addComment
 * @static
 * @param {Object} query - plain mongo query object.
 * @param {String} comment
 * @return {Object}
 */
exports.addComment = function(query, comment) {
	query = _.omit(query, '$comment');
	return (comment) ? objtools.merge({ $comment: comment }, query) : query;
};

/**
 * Gets the query info from the provided op. Handles getMore operations using
 * the originatingCommand property.
 *
 * @method getQuery
 * @static
 * @param {Object} op
 * @return {Object|String} - Query info, which may be an object or a truncated
 *   string representation (in mongo <3.5).
 */
exports.getQuery = function(op) {
	let { query } = op;
	return (query && query.getMore) ? op.originatingCommand : query;
};

/**
 * Gets the top-level $comment field, if any, from the provided query info
 * object. Supports find and count operations, as well as aggregate operations
 * where the comment occurs in a $match phase at the beginning of the pipeline.
 * Also supports mongo 3.5-style truncated queries, as mentioned here:
 * https://jira.mongodb.org/browse/DOCS-10060
 *
 * @method getCommentFromQuery
 * @static
 * @param {Object} query
 * @param {String|Null} - Will return null if no comment can be found.
 */
exports.getCommentFromQuery = function(query) {
	let commentPath;
	if (query.find) {
		commentPath ='filter.$comment';
	} else if (query.count) {
		commentPath = 'query.$comment';
	} else if (query.aggregate) {
		commentPath = 'pipeline.0.$match.$comment';
	} else if (query.$truncated) {
		commentPath = 'comment';
	} else {
		return null;
	}

	return objtools.getPath(query, commentPath) || null;
};

/**
 * Checks the provided query info for the presence of the provided comment.
 *
 * @method queryHasComment
 * @static
 * @param {Object|String} query
 * @param {String} comment
 * @return {Boolean} - True if the comment is present, false otherwise.
 */
exports.queryHasComment = function(query, comment) {
	// TODO: Remove this check when compatibility with mongo <3.5 is no longer needed.
	if (_.isString(query)) {
		let commentPattern = _.escapeRegExp(comment).replace(/"/g, '\\\\?"');
		return new RegExp(`\\$comment:\\s*"${commentPattern}"`).test(query);
	}

	return exports.getCommentFromQuery(query) === comment;
};

/**
 * Checks the provided op object for the presence of the provided comment.
 *
 * @method hasComment
 * @static
 * @param {Object} op
 * @param {String} comment
 * @return {Boolean} - True if the comment is present, false otherwise.
 */
exports.hasComment = function(op, comment) {
	let query = exports.getQuery(op);
	return exports.queryHasComment(query, comment);
};

/**
 * Searches through the currentOp result document and returns the opIds
 * for all in-progress operations with the provided comment.
 *
 * @method getOpIdsWithComment
 * @param {Object} currentOpDoc - currentOp command result.
 * @param {String} comment
 * @return {Array{String}}
 */
exports.getOpIdsWithComment = function(currentOpDoc, comment) {
	let opIds = [];
	for (let op of currentOpDoc.inprog) {
		if (exports.hasComment(op, comment)) {
			opIds.push(op.opid);
		}
	}

	return opIds;
};
