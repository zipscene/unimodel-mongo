// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const _ = require('lodash');
const XError = require('xerror');
const { SchemaDocument } = require('unimodel-core');
const objtools = require('objtools');
const commonQuery = require('common-query');
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
 *   @param {String[]} options.fields - If this is a partial document, this is the list
 *     of fields that were retrieved with that document.
 */
class MongoDocument extends SchemaDocument {

	constructor(model, data, options = {}) {
		const profSuper = model.profiler.begin('MongoDocument#constructor > super');
		super(model, data);
		profSuper.end();

		this.options = options;
		this.profiler = model.profiler;
		if (this.options.isExisting) {
			this._originalMongoData = objtools.deepCopy(data);
			delete this._originalMongoData._id;
		}

		const profPrepare = model.profiler.begin('MongoDocument#constructor @ _prepareNormalizeData');
		let fields = this._prepareNormalizeData(data);
		profPrepare.end();

		if (fields.__rev) {
			this._revisionNumber = fields.__rev;
		} else if (!this.options.isExisting) {
			this._revisionNumber = 1;
		}

		// Pull `_id` out into the root of the instance
		if (fields._id) {
			this._id = fields._id;
			this._originalId = fields._id;
		}

		if (this.options.isExisting) {
			const profCopy = model.profiler.begin('MongoDocument#constructor > deepCopy');
			this._originalData = objtools.deepCopy(data);
			profCopy.end();
		} else {
			this._originalData = null;
		}

		if (this.options.fields) {
			this.fields = this.options.fields;
		}

		// Call the post-init hook, as initialization has finished
		const profTrigger = model.profiler.begin('MongoDocument#constructor > triggerSync:post-init');
		model.triggerSync('post-init', this);
		profTrigger.end();
	}

	/**
	 * Remove __rev, _id, and map indexes from the given data object.
	 *
	 * @method _prepareNormalizeData
	 * @private
	 * @param {Object} data - The data to modify
	 * @return {Object} - An object containing all removed fields
	 */
	_prepareNormalizeData(data) {
		let fields = {};
		if (data.__rev !== undefined) {
			fields.__rev = data.__rev;
			delete data.__rev;
		}
		if (data._id !== undefined) {
			fields._id = data._id;
			delete data._id;
		}
		for (let field in data) {
			if (_.startsWith(field, '_mapidx_') || _.startsWith(field, '_geoidx_')) {
				fields[field] = data[field];
				delete data[field];
			}
		}
		return fields;
	}

	_normalizeData(options = {}) {
		let data = this.data;

		const profPrepare = this.model.profiler.begin('MongoDocument#_normalizeData @ _prepareNormalizeData');
		let fields = this._prepareNormalizeData(data);
		profPrepare.end();

		const profNormalize = this.model.profiler.begin('MongoDocument#_normalizeData @ _normalizeData');
		let result = super._normalizeData(options);
		profNormalize.end();

		const profMerge = this.model.profiler.begin('MongoDocument#_normalizeData > merge');
		objtools.merge(data, fields);
		profMerge.end();

		return result;
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
	 * Return unique id
	 *
	 * @method getUniqueId
	 * @return {Mixed}
	 */
	getUniqueId() {
		let field = this.model.options.uniqueIdField;
		if (field === '_id') return this.getInternalId();
		return this.data[field];
	}

	/**
	 * Set unique id
	 *
	 * @method setUniqueId
	 * @param {String} newId
	 * @return {MongoDocument}
	 */
	setUniqueId(newId) {
		let field = this.model.options.uniqueIdField;
		if (field === '_id') return this.setInternalId(newId);
		this.data[field] = newId;
		return this;
	}

	/**
	 * Returns an array of fields that were modified at the root object level
	 * between the two given objects.
	 *
	 * @method _getRootModifiedFields
	 * @protected
	 * @param {Object} originalData
	 * @param {Object} newData
	 * @return {String[]} - Array of field names
	 */
	_getRootModifiedFields(originalData, newData) {
		if (!_.isObject(originalData)) return _.keys(newData || {});
		if (!_.isObject(newData)) return _.keys(originalData);
		let fields = [];
		for (let field in originalData) {
			if (!(field in newData) || !objtools.deepEquals(newData[field], originalData[field])) {
				fields.push(field);
			}
		}
		for (let field in newData) {
			if (!(field in originalData)) {
				fields.push(fields);
			}
		}
		return fields;
	}

	/**
	 * Returns a list of fields to ignore when checking if the document is modified
	 * for saving.
	 *
	 * @method _getIgnoreSaveModifiedFields
	 * @protected
	 * @return {String[]}
	 */
	_getIgnoreSaveModifiedFields() {
		return [ '__rev' ];
	}

	/**
	 * Checks to see if a save is actually needed (ie, if data was actually modified).
	 *
	 * @method _checkSaveNeeded
	 * @protected
	 * @param {Object} originalData
	 * @param {Object} newData
	 * @return {Boolean}
	 */
	_checkSaveNeeded(originalData, newData) {
		let modifiedFields = this._getRootModifiedFields(originalData, newData);
		let allowedModifiedFields = this._getIgnoreSaveModifiedFields();
		return !!(_.difference(modifiedFields, allowedModifiedFields).length);
	}

	/**
	 * Save document to database
	 *
	 * @method save
	 * @return {MongoDocument}
	 * @throws {XError}
	 * @param {Object} [options]
	 *   @param {Boolean} [options.force] - Force saving even if not modified
	 * @since v0.0.1
	 */
	save(options = {}) {
		let collection, documentData, mongoDocumentData;
		let prof = this.model.profiler.begin('#save');

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
				// This is the new, normalized, deserialized data object, which will be accessible after the save
				documentData = this.model.schema.normalize(objtools.deepCopy(mongoDocumentData));
				// Set the revision
				mongoDocumentData.__rev = this._revisionNumber;
				// Add the indexed map arrays to the normalized data
				this.model.normalizeDocumentIndexedMapValues(mongoDocumentData);
				// Add indexed polytokens
				this.model.normalizeDocumentIndexedGeoHashedValues(mongoDocumentData);
			})
			.then(() => {
				let _id = this.getInternalId();
				let id = this.getUniqueId();

				// If no id exists, save as a new document
				if (typeof this._originalData === 'undefined' || typeof _id === 'undefined') {
					// If new, save as a new document and update the stored _id
					return collection.insertOne(mongoDocumentData)
						.then((result) => {
							// Update the instance's id
							this.setInternalId(result.ops[0]._id);
						});
				}

				// If the id has changed, insert data as a new document
				if (_id !== this._originalId) {
					mongoDocumentData._id = _id;

					// Insert new document and remove the old document
					return collection.insertOne(mongoDocumentData)
						.then(() => this.remove());
				}

				// Increment revision number
				if (_.isNumber(mongoDocumentData.__rev)) {
					mongoDocumentData.__rev += 1;
				} else {
					mongoDocumentData.__rev = 1;
				}

				if (
					!options.force &&
					mongoDocumentData.__rev > 1 &&
					!this._checkSaveNeeded(this._originalData, mongoDocumentData)
				) {
					mongoDocumentData.__rev--;
					return;
				}

				// Generate an update expression from the existing model data to the new data
				let update = commonQuery.Update.createFromDiff(
					this._originalMongoData,
					mongoDocumentData,
					{ replaceArrays: 'SMALLER' }
				);

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
				let updateFn = 'updateOne';
				if (Object.keys(update).length > 0 && Object.keys(update)[0][0] !== '$') {
					// If doing a full replacement of the object, need to use this function
					updateFn = 'replaceOne';
				}

				return collection.replaceOne({
					[this.model.options.uniqueIdField]: id,
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
								[this.model.options.uniqueIdField]: id,
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
				this._originalMongoData = mongoDocumentData;
				delete this._originalMongoData._id;

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
			.then(() => this)
			.then(prof.wrappedEnd(), prof.wrappedEndError());
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
		let prof = this.model.profiler.begin('#remove');

		return this.model.collectionPromise
			.then((_collection) => {
				collection = _collection;
			})
			.then(() => this.model.trigger('pre-remove', this))
			.then(() => {
				return collection.removeOne({
					_id: this._originalId,
					__rev: this._revisionNumber
				});
			})
			.then(() => this.model.trigger('post-remove', this))
			.then(() => {
				// Update instance data
				delete this._originalId;

				return this;
			})
			.then(prof.wrappedEnd(), prof.wrappedEndError());
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
