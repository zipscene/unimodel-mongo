const _ = require('lodash');
const pasync = require('pasync');
const XError = require('xerror');
const SchemaDocument = require('zs-unimodel').SchemaDocument;
const objtools = require('zs-objtools');
const commonQuery = require('zs-common-query');

/**
 * MongoDB document class for Unimodel.
 *
 * @class UnimongoDocument
 * @constructor
 * @param {UnimongoModel} model
 * @param {Object} data
 * @param {Object} [options] - Additional options
 *   @param {Boolean} options.isExisting - Whether instance is an existing document
 *   @param {Boolean} options.isPartial - Whether instance is a partial document
 */
class UnimongoDocument extends SchemaDocument {

	constructor(model, data, options = {}) {
		super(model, data);

		this.options = options;

		if (data.__rev) {
			this._revisionNumber = data.__rev;
			delete data.__rev;
		} else if (!this.options.isExisting) {
			this._revisionNumber = 1;
		}

		// Pull `_id` out into the root of the instance
		if (data._id) {
			this._id = data._id;
			this._originalId = data._id;
			delete data._id;
		}

		if (this.options.isExisting) {
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
	 * @throws {XError}
	 * @since v0.0.1
	 */
	save() {
		let collection, normalizedData;

		if (this.options.isPartial && !this.model.options.allowSavingPartials) {
			throw new XError(
				XError.UNSUPPORTED_OPERATION,
				'Attempting to save a partial document, which is disallowed by the model.'
            );
		}

		return this.model.collectionPromise
			.then((_collection) => {
				collection = _collection;
			})
			.then(() => this.model.trigger('pre-normalize'))
			.then(() => {
				// Normalize model data according to schema
				normalizedData = this.model.schema.normalize(this.data);
				normalizedData.__rev = this._revisionNumber;
			})
			.then(() => this.model.trigger('post-normalize'))
			.then(() => this.model.trigger('pre-save'))
			.then(() => {
				let _id = this.getInternalId();

				// Test whether this is a new document
				if (typeof this._originalData === 'undefined' || typeof _id === 'undefined') {
					// If new, save as a new document and update the stored _id
					return collection.insert(normalizedData)
						.then((result) => {
							let data = result.ops[0];

							this.setInternalId(data._id);

							return data;
						});
				} else {
					if (_id !== this._originalId) {
						// If the _id has changed, execute a remove and insert

						// Remove the old document
						return this.model.remove({ _id: this._originalId })
							// Check for a conflicting _id
							.then(() => this.model.count({ _id }))
							.then((count) => {
								if (count !== 0) {
									throw new XError(
										XError.ALREADY_EXISTS,
										'New `_id` conflicts with existing document.'
                                    );
								}

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
						let update = commonQuery.Update.createFromDiff(this._originalData, normalizedData);

						let pushOperations;
						if (update.$push) {
							pushOperations = { $push: update.$push };
							delete update.$push;
						}

						let updateCalls = [
							collection.updateOne({
								_id,
								__rev: this._revisionNumber
							}, update)
						];

						if (pushOperations) {
							updateCalls.push(
								collection.updateOne({
									_id,
									__rev: this._revisionNumber
								}, pushOperations)
							);
						}

						// Execute the update expression
						return pasync.all(updateCalls)
							.then((results) => {
								if (results[0].result.nModified < 1) {
									throw new XError(
										XError.NOT_FOUND,
										'The document does not exist, or was updated elsewhere.'
                                    );
								}
							});
					}
				}
			})
			.then(() => {
				// Update instance data
				this._revisionNumber = normalizedData.__rev;
				this.setInternalId(normalizedData._id);
				this._originalId = normalizedData._id;
				delete normalizedData.__rev;
				delete normalizedData._id;
				this._originalData = normalizedData;
				this.data = objtools.deepCopy(normalizedData);
			})
			.then(() => this.model.trigger('post-save'))
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
			})
			.then(() => this.model.trigger('pre-remove'))
			.then(() => {
				return collection.remove({
					_id: this._originalId,
					__rev: this._revisionNumber
				});
			})
			.then(() => this.model.trigger('post-remove'))
			.then(() => {
				// Update instance data
				delete this._originalId;

				return this;
			});
	}

}

module.exports = UnimongoDocument;
