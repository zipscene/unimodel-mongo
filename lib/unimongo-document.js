const _ = require('lodash');
const XError = require('xerror');
const SchemaDocument = require('zs-unimodel').SchemaDocument;
const objtools = require('zs-objtools');
const commonQuery = require('zs-common-query');
const UnimongoError = require('./unimongo-error');

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
		if (this.options.isPartial && !this.model.options.allowSavingPartials) {
			throw new XError(
				XError.UNSUPPORTED_OPERATION,
				'Attempting to save a partial document, which is disallowed by the model.'
            );
		}

		let collection, normalizedData;
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

				// If no id exists, save as a new document
				if (typeof this._originalData === 'undefined' || typeof _id === 'undefined') {
					// If new, save as a new document and update the stored _id
					return collection.insert(normalizedData)
						.then((result) => {
							// Update the instance's id
							this.setInternalId(result.ops[0]._id);
						});
				}

				// If the id has changed, insert data as a new document
				if (_id !== this._originalId) {
					normalizedData._id = _id;

					// Insert new document and remove the old document
					return collection.insert(normalizedData)
						.then(() => this.remove());
				}

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
					// Split out the $push operations,
					// and increment the revision number again for them as well
					pushOperations = {
						$set: { __rev: normalizedData.__rev + 1 },
						$push: update.$push
					};
					delete update.$push;
				}

				// Execute the update expression
				return collection.updateOne({
					_id,
					__rev: this._revisionNumber
				}, update)
					.then((result) => {
						if (result.result.nModified < 1) {
							throw new XError(
								XError.NOT_FOUND,
								'The document does not exist, or was updated elsewhere.'
							);
						}

						if (pushOperations) {
							return collection.updateOne({
								_id,
								__rev: normalizedData.__rev
							}, pushOperations)
								.then((result) => {
									if (result.result.nModified < 1) {
										throw new XError(
											XError.NOT_FOUND,
											'The document does not exist, or was updated elsewhere.'
										);
									}

									// Increment revision number
									normalizedData.__rev += 1;
								});
						}
					})
					.catch((err) => {
						// Reset revision number
						normalizedData.__rev -= 1;
						throw err;
					});
			})
			.catch((err) => this._handleMongoErrors(err))
			.then(() => {
				// Update instance data
				this._revisionNumber = normalizedData.__rev;
				if (normalizedData._id) {
					this.setInternalId(normalizedData._id);
					this._originalId = normalizedData._id;
				}

				delete normalizedData.__rev;
				delete normalizedData._id;

				this._originalData = normalizedData;
				this.data = objtools.deepCopy(normalizedData);
			}, (err) => {
				// Update instance data
				this._revisionNumber = normalizedData.__rev;
				this.setInternalId(normalizedData._id);
				delete normalizedData.__rev;
				delete normalizedData._id;
				this.data = normalizedData;

				throw err;
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

	/**
	 * Wrap mongo errors
	 *
	 * @method _handleMongoErrors
	 * @return {UnimongoError}
	 * @since v0.0.1
	 */
	_handleMongoErrors(err) {
		throw UnimongoError.fromMongoError(err, this.model);
	}

}

module.exports = UnimongoDocument;
