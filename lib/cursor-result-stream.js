// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const zstreams = require('zstreams');

class CursorResultStream extends zstreams.Transform {

	constructor(model, cursor, options = {}) {
		super({ objectMode: true });

		this._cursorResultStream = {
			model,
			options
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
			newDoc = this._cursorResultStream.model._createExisting(chunk, this._cursorResultStream.options);
		} catch (ex) {
			return cb(ex);
		}
		this.push(newDoc);
		cb();
	}

}

module.exports = CursorResultStream;
