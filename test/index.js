// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const expect = require('chai').expect;
const mongo = require('../lib/index');
const testScaffold = require('./lib/mongo-scaffold');

describe('zs-unimodel-mongo', () => {
	beforeEach(testScaffold.resetAndConnect);

	it('should register and be able to get model', () => {
		let model = mongo.createModel('Foo', {
			id: { type: String, index: true, id: true },
			name: { type: String, index: true }
		});

		return model.collectionPromise
			.then(() => {
				mongo.model(model);
				let GotModel = mongo.model('Foo');
				expect(GotModel).to.be.instanceOf(mongo.MongoModel);
				expect(GotModel.getName()).to.equal('Foo');
			});
	});

	it('should throw error when passed in non model instance nor string', () => {
		try {
			mongo.model(1);
		} catch (ex) {
			expect(ex.code).to.equal('invalid_argument');
			expect(ex.message).to.equal('argument must either be a string or a model instance');
		}
	});

	it('should return error when trying to get non-existing model', () => {
		try {
			mongo.model('NonExist');
		} catch (ex) {
			expect(ex.code).to.equal('internal_error');
			expect(ex.message).to.equal('Model not found: NonExist');
		}
	});
});
