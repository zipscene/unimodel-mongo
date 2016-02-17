const XError = require('xerror');

/**
 * Error class for errors from mongo.  Constructor takes same arguments as XError.
 *
 * @class MongoError
 * @constructor
 */
class MongoError extends XError {

	constructor(...args) {
		super(...args);
	}

	/**
	 * Converts a mongo error into a MongoError.
	 *
	 * @method fromMongoError
	 * @static
	 * @param {Object} err - Error from mongo
	 * @param {MongoModel} [model] - Model to use in error parsing
	 * @return {MongoError}
	 */
	static fromMongoError(err, model) {
		if (err.name !== 'MongoError') return err;

		// Convert error from exceeding maxTimeMS.
		// This incoming error should have a 'code' attribute of 50 according to Mongo's documenation,
		// but in MongoModel#find, the code is undefined at the moment cusor.toArray() throws it. This
		// appears to be a bug in Mongo. As a workaround, the message text is examined instead.
		if (/operation exceeded time limit/.test(err.message)) {
			return new MongoError(XError.TIMED_OUT, 'Database operation timed out', err);
		}

		if (!err.code) {
			let message = 'Internal database error';
			if (err.message) message += `: ${err.message}`;

			return new MongoError(XError.DB_ERROR, message, err);
		}

		let data = {};
		let codeMap = {
			11000: XError.ALREADY_EXISTS,
			11001: XError.ALREADY_EXISTS
		};

		let code = codeMap[err.code] || XError.DB_ERROR;

		// If the error is a duplicate key, try to detect the conflicting key
		if (code === XError.ALREADY_EXISTS && model) {
			data.keys = [];

			let pattern = /E1100[01] duplicate key error index: [^.]+\.[^.]+\.\$([^\s]+)\s+dup key/;
			let matches = err.message.match(pattern);

			if (matches) {
				let [ , indexName ] = matches;

				model.indexes.forEach((index) => {
					if (index.name === indexName) {
						data.keys.push(...Object.keys(index.key));
					}
				});
			}
		}

		return new MongoError(code, data, err);
	}

}

// Register the XError code with default message
XError.registerErrorCode('db_error', {
	message: 'Internal database error',
	http: 500
});

module.exports = MongoError;
