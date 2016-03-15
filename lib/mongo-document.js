const _ = require('lodash');
const XError = require('xerror');
const { SchemaDocument } = require('zs-unimodel');
const objtools = require('zs-objtools');
const commonQuery = require('zs-common-query');
const MongoError = require('./mongo-error');

/**
 * MongoDB document class for Unimodel.
 *
 * @class MongoDocument
 * @constructor
 * @param {MongoModel} model
 * @param {Object} data
 * @param {Object} [options] - Additional options
 *   @param {Boolean} options.isExisting - Whether instance is an existing document
 *   @param {Boolean} options.isPartial - Whether instance is a partial document
 */
class MongoDocument extends SchemaDocument {

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

		// Strip out "map indexes"
		for (let field in data) {
			if (/^_mapidx_/.test(field)) {
				delete data[field];
			}
		}

		if (this.options.isExisting) {
			this._originalData = objtools.deepCopy(data);
		} else {
			this._originalData = null;
		}

		// Call the post-init hook, as initialization has finished
		model.triggerSync('post-init', this);
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
	 * @return {MongoDocument}
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
	 * @return {MongoDocument}
	 * @throws {XError}
	 * @since v0.0.1
	 */
	save() {
		let collection, documentData, mongoDocumentData;
		return this.model.collectionPromise
			.then((_collection) => {
				collection = _collection;
			})
			.then(() => {
				if (this.options.isPartial && !this.model.options.allowSavingPartials) {
					let msg = 'Attempting to save a partial document, which is disallowed by the model.';
					throw new XError(XError.UNSUPPORTED_OPERATION, msg);
				}
			})
			.then(() => this.model.trigger('pre-normalize', this))
			.then(() => {
				// Normalize model data according to schema
				this.data = mongoDocumentData = this.model.schema.normalize(this.data, { serialize: true });
			})
			.then(() => this.model.trigger('post-normalize', this))
			.then(() => this.model.trigger('pre-save', this) )
			.then(() => {
				// This is the new, normalized data object, which will be accessible after the save
				documentData = objtools.deepCopy(mongoDocumentData);
				// Set the revision
				mongoDocumentData.__rev = this._revisionNumber;
				// Add the indexed map arrays to the normalized data
				this.model.normalizeDocumentIndexedMapValues(mongoDocumentData);
			})
			.then(() => {
				let _id = this.getInternalId();

				// If no id exists, save as a new document
				if (typeof this._originalData === 'undefined' || typeof _id === 'undefined') {
					// If new, save as a new document and update the stored _id
					return collection.insert(mongoDocumentData)
						.then((result) => {
							// Update the instance's id
							this.setInternalId(result.ops[0]._id);
						});
				}

				// If the id has changed, insert data as a new document
				if (_id !== this._originalId) {
					mongoDocumentData._id = _id;

					// Insert new document and remove the old document
					return collection.insert(mongoDocumentData)
						.then(() => this.remove());
				}

				// Increment revision number
				if (_.isNumber(mongoDocumentData.__rev)) {
					mongoDocumentData.__rev += 1;
				} else {
					mongoDocumentData.__rev = 1;
				}

				// Generate an update expression from the existing model data to the new data
				let update = commonQuery.Update.createFromDiff(this._originalData, mongoDocumentData);

				let pushOperations;
				if (update.$push) {
					// Split out the $push operations,
					// and increment the revision number again for them as well
					pushOperations = {
						$set: { __rev: mongoDocumentData.__rev + 1 },
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
								XError.CONFLICT,
								'The document was updated elsewhere.'
							);
						}

						if (pushOperations) {
							return collection.updateOne({
								_id,
								__rev: mongoDocumentData.__rev
							}, pushOperations)
								.then((result) => {
									if (result.result.nModified < 1) {
										throw new XError(
											XError.CONFLICT,
											'The document was updated elsewhere.'
										);
									}

									// Increment revision number
									mongoDocumentData.__rev += 1;
								});
						}
					})
					.catch((err) => {
						// Reset revision number
						mongoDocumentData.__rev -= 1;
						throw err;
					});
			})
			.catch((err) => this._handleMongoErrors(err))
			.then(() => {
				// Update instance data
				this._revisionNumber = mongoDocumentData.__rev;
				if (mongoDocumentData._id) {
					this.setInternalId(mongoDocumentData._id);
					this._originalId = mongoDocumentData._id;
				}

				this._originalData = documentData;
				this.data = objtools.deepCopy(documentData);
			}, (err) => {
				// Update instance data
				if (mongoDocumentData) {
					this._revisionNumber = mongoDocumentData.__rev;
					this.setInternalId(mongoDocumentData._id);
				}
				if (documentData) {
					this.data = documentData;
				}

				throw err;
			})
			.then(() => this.model.trigger('post-save', this))
			.then(() => this);
	}

	/**
	 * Remove document from database
	 *
	 * @method remove
	 * @return {MongoDocument}
	 * @since v0.0.1
	 */
	remove() {
		let collection;

		return this.model.collectionPromise
			.then((_collection) => {
				collection = _collection;
			})
			.then(() => this.model.trigger('pre-remove', this))
			.then(() => {
				return collection.remove({
					_id: this._originalId,
					__rev: this._revisionNumber
				});
			})
			.then(() => this.model.trigger('post-remove', this))
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
	 * @return {MongoError}
	 * @since v0.0.1
	 */
	_handleMongoErrors(err) {
		throw MongoError.fromMongoError(err, this.model);
	}

	isPartial() {
		return !!this.options.isPartial;
	}

}

module.exports = MongoDocument;
