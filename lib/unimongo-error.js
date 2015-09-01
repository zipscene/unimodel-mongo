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
	 * Converts a mongo error into a UnimongoError.
	 *
	 * @method fromMongoError
	 * @static
	 * @param {Object} err - Error from mongo
	 * @param {UnimongoModel} [model] - Model to use in error parsing
	 * @return {UnimongoError}
	 */
	static fromMongoError(err, model) {
		if (err.name !== 'MongoError') return err;

		if (!err.code) {
			let message = 'Internal database error';
			if (err.message) message += `: ${err.message}`;

			return new UnimongoError(XError.DB_ERROR, message, err);
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

			let pattern = /^E1100[01] duplicate key error index: [^.]+\.[^.]+\.\$([^ ]+) dup key/;
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

		return new UnimongoError(code, data, err);
	}

}

// Register the XError code with default message
XError.registerErrorCode('db_error', {
	message: 'Internal database error',
	http: 500
});

module.exports = UnimongoError;
