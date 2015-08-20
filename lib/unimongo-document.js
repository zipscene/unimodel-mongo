const SchemaDocument = require('zs-unimodel').SchemaDocument;
const objtools = require('zs-objtools');

class UnimongoDocument extends SchemaDocument {

	constructor(model, data, isExistingDocument) {
		super(model, data);
		if (data.__rev) {
			this._revisionNumber = data.__rev;
			delete data.__rev;
		} else {
			this._revisionNumber = 1;
		}
		// TODO: If the _id field exists in `data`, remove it and instead store it on `this` instead of `data`.
		// Will also need to store an _originalId field for diffing _id
		if (isExistingDocument) {
			this._originalData = objtools.deepCopy(data);
		} else {
			this._originalData = null;
		}
	}

	getInternalId() {
	}

	setInternalId(newId) {
	}

	save() {
		// Not in any particular order:
		// - Normalize model data according to schema
		// - Call the relevant model hooks (pre/post normalize, pre/post save)
		// - If the document is a new document (no this._originalData) save it as a new document and update the stored _id
		// - If the document is an existing document:
		//   - If the _id has changed, execute a remove and insert.  Check for (and error on) a conflicting _id.
		//   - Generate a mongo update expression from the existing model data to the new data.  In this update expression,
		//     include an increment to __rev .
		//   - Execute the update expression.  The query to run the update on should include this document's _id as well as
		//     __rev: this._revisionNumber .
		//   - Check the resultant numModified field.  If nothing was modified, then the document was either deleted or
		//     the revision was updated, and is an error (a conflict).
	}

	remove() {
		// - If the document's _id has changed, instead use the original _id
		// - Execute pre/post remove hooks
		// - Execute a mongo remove command that includes the original _id and current __rev
	}

}

module.exports = UnimongoDocument;
