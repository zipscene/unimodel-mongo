// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const chai = require('chai');
const expect = chai.expect;
const { MongoError, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');
const XError = require('xerror');

chai.use(require('chai-as-promised'));

describe('MongoError', function() {
	beforeEach(testScaffold.resetAndConnect);
	after(testScaffold.reset);

	it('should parse error on duplicated id', function() {
		let model = createModel('testings', { foo: String });

		let collection;
		let promise = model.collectionPromise
			.then((_collection) => { collection = _collection; })
			.then(() => collection.insertOne({ _id: 'some id', foo: 'bar' }))
			.then(() => collection.insertOne({ _id: 'some id', foo: 'baz' }))
			.catch((err) => {
				throw MongoError.fromMongoError(err, model);
			})
			.then(() => {
				throw new XError(XError.INTERNAL_ERROR, 'Expected rejection');
			}, (err) => {
				expect(err).to.be.an.instanceOf(MongoError);
				expect(err.data.keys.length).to.equal(1);
				expect(err.data.keys[0]).to.equal('_id');
			});

		return promise;
	});

	it('should parse error on duplicated keys', function() {
		let model = createModel('testings', { foo: String, bar: String });
		model.index({ foo: 1, bar: 1 }, { unique: true });

		let collection;
		let promise = model.collectionPromise
			.then((_collection) => { collection = _collection; })
			.then(() => collection.insertOne({ foo: 'one', bar: 'two' }))
			.then(() => collection.insertOne({ foo: 'one', bar: 'two' }))
			.catch((err) => {
				throw MongoError.fromMongoError(err, model);
			})
			.then(() => {
				throw new XError(XError.INTERNAL_ERROR, 'Expected rejection');
			}, (err) => {
				expect(err).to.be.an.instanceOf(MongoError);
				expect(err.data.keys.length).to.equal(2);
				expect(err.data.keys[0]).to.equal('foo');
				expect(err.data.keys[1]).to.equal('bar');
			});

		return promise;
	});

	it('should parse error on duplicated keys in update', async function() {
		let model = createModel('testings', { foo: String, bar: String });
		model.index({ foo: 1, bar: 1 }, { unique: true });

		let collection = await model.collectionPromise;
		await collection.insertOne({ foo: 'one', bar: 'two' });
		await collection.insertOne({ foo: 'one', bar: 'three' });
		try {
			await model.update({ bar: 'three' }, { $set: { bar: 'two' } }, { forceAtomic: true });
		} catch (err) {
			expect(err).to.be.an.instanceOf(MongoError);
			expect(err.code).to.equal(XError.ALREADY_EXISTS);
			expect(err.data.keys.length).to.equal(2);
			expect(err.data.keys[0]).to.equal('foo');
			expect(err.data.keys[1]).to.equal('bar');
			return;
		}
		throw new XError(XError.INTERNAL_ERROR, 'Expected rejection');
	});

});
