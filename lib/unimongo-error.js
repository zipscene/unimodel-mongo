const XError = require('xerror');

/**
 * Error class for errors from mongo.  Constructor takes same arguments as XError.
 *
 * @class UnimongoError
 * @constructor
 */
class UnimongoError extends XError {

	constructor(...args) {
		super(...args);
	}

	/**
	 * Converts a mongo error from mongo into a UnimongoError.
	 *
	 * @method fromMongoError
	 * @static
	 * @param {Object} err - Error from mongo
	 * @return {UnimongoError}
	 */
	static fromMongoError(err) {
		if (err.message) {
			return new UnimongoError(XError.DB_ERROR, 'Internal database error: ' + err.message, err);
		} else {
			return new UnimongoError(XError.DB_ERROR, 'Internal database error', err);
		}
	}

}

// Register the XError code with default message
XError.registerErrorCode('db_error', {
	message: 'Internal database error',
	http: 500
});

module.exports = UnimongoError;
