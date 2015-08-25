const Transform = require('zstreams').Transform;
const zstreams = require('zstreams');

class CursorResultStream extends Transform {

	constructor(model, cursor, options = {}) {
		super({ objectMode: true });

		this.options = options;

		this._cursorResultStream = {
			model
		};

		if (cursor) {
			this.setCursor(cursor);
		}
	}

	setCursor(cursor) {
		if (this._cursorResultStream.cursor) {
			throw new Error('Already have a cursor');
		}
		this._cursorResultStream.cursor = cursor;
		zstreams(cursor).pipe(this);
	}

	_transform(chunk, encoding, cb) {
		let newDoc;
		try {
			newDoc = this._cursorResultStream.model._createExisting(chunk, this.options);
		} catch (ex) {
			return cb(ex);
		}
		this.push(newDoc);
		cb();
	}

}

module.exports = CursorResultStream;
