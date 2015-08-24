const _ = require('lodash');
const SchemaDocument = require('zs-unimodel').SchemaDocument;
const objtools = require('zs-objtools');
const commonQuery = require('zs-common-query');

class UnimongoDocument extends SchemaDocument {

	constructor(model, data, isExistingDocument) {
		super(model, data);

		if (data.__rev) {
			this._revisionNumber = data.__rev;
			delete data.__rev;
		} else if (!isExistingDocument) {
			this._revisionNumber = 1;
		}

		// Pull `_id` out into the root of the instance
		if (data._id) {
			this._id = data._id;
			this._originalId = data._id;
			delete data._id;
		}

		if (isExistingDocument) {
			this._originalData = objtools.deepCopy(data);
		} else {
			this._originalData = null;
		}
	}

	/**
	 * Return internal id
	 *
	 * @method getInternalId
	 * @return {String}
	 * @since v0.0.1
	 */
	getInternalId() {
		return this._id;
	}

	/**
	 * Set internal id
	 *
	 * @method getInternalId
	 * @param {String} newId
	 * @return {UnimongoDocument}
	 * @since v0.0.1
	 */
	setInternalId(newId) {
		this._id = newId;
		return this;
	}

	/**
	 * Save document to database
	 *
	 * @method save
	 * @return {UnimongoDocument}
	 * @since v0.0.1
	 */
	save() {
		let collection;
		let normalizedData;

		return this.model.collectionPromise
			.then((_collection) => {
				collection = _collection;

				return this.model.trigger('pre-normalize');
			})
			.then(() => {
				// Normalize model data according to schema
				normalizedData = this.model.schema.normalize(this.data);
				normalizedData.__rev = this._revisionNumber;
				return this.model.trigger('post-normalize');
			})
			.then(() => this.model.trigger('pre-save'))
			.then(() => {
				// Test whether this is a new document
				if (_.isUndefined(this._originalData)) {
					// If new, save as a new document and update the stored _id
					return collection.insert(normalizedData)
						.then((result) => {
							this.setInternalId(result._id);
							return this;
						});
				} else {
					let _id = this.getInternalId();

					// If the _id has changed, execute a remove and insert
					if (_id !== this._originalId) {
						// Remove the old document
						return this.model.remove({ _id: this._originalId })
							.then(() => {
								// Check for a conflicting _id
								return this.model.count({ _id });
							})
							.then((count) => {
								// TODO: Make error better
								if (count !== 0) throw new Error('New _id conflicts with existing document.');

								// Insert data as a new document
								return collection.insert(normalizedData);
							})
							.then((result) => {
								this.setInternalId(result._id);
							});
					} else {
						// Increment revision number
						if (_.isNumber(normalizedData.__rev)) {
							normalizedData.__rev += 1;
						} else {
							normalizedData.__rev = 1;
						}

						// Generate an update expression from the existing model data to the new data
						let update = commonQuery.Update.createFromDiff(this._originalData, normalizedData, {
							replaceArrays: 'SMALLER'
						});

						// Execute the update expression
						return collection.updateOne({
							_id,
							__rev: this._revisionNumber
						}, update)
							.then((result) => {
								// TODO: Make error better
								if (result.modifiedCount !== 1) {
									throw new Error('The document does not exist, or was updated elsewhere.');
								}
							});
					}
				}
			})
			.then(() => {
				// Update instance data
				this._revisionNumber = normalizedData.__rev;
				delete normalizedData.__rev;
				this._originalData = normalizedData;
				this.data = normalizedData;

				return this.model.trigger('post-save');
			})
			.then(() => this);
	}

	/**
	 * Remove document from database
	 *
	 * @method remove
	 * @return {UnimongoDocument}
	 * @since v0.0.1
	 */
	remove() {
		let collection;

		return this.model.collectionPromise
			.then((_collection) => {
				collection = _collection;

				return this.model.trigger('pre-remove');
			})
			.then(() => {
				return collection.remove({
					_id: this._originalId,
					__rev: this._revisionNumber
				});
			})
			.then(() => this.model.trigger('post-remove'))
			.then(() => {
				// Update instance data
				delete this._id;
				delete this._originalId;
			})
			.then(() => this);
	}

}

module.exports = UnimongoDocument;
