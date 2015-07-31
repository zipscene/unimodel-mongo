const expect = require('chai').expect;
const createModel = require('../lib').createModel;
const UnimongoDb = require('../lib').UnimongoDb;
const UnimongoError = require('../lib').UnimongoError;
const UnimongoModel = require('../lib').UnimongoModel;
const pasync = require('pasync');
const testScaffold = require('./lib/mongo-scaffold');

describe('UnimongoModel', function() {

	beforeEach(testScaffold.resetAndConnect);

	it('should create the collection when it doesnt exist', function() {
		let model = createModel('testings', { foo: String });
		return model.collectionPromise;
	});

	it('should not fail if the collection already exists', function() {
		let model1 = createModel('testings', { foo: String });
		return model1.collectionPromise
			.then(() => {
				let model2 = createModel('testings', { foo: String });
				return model2.collectionPromise;
			});
	});

});



