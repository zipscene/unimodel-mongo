const chai = require('chai');
const expect = chai.expect;
const { UnimongoError, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');

chai.use(require('chai-as-promised'));

describe('UnimongoError', function() {

	beforeEach(testScaffold.resetAndConnect);

	it('should error on duplicated keys', function() {
		let model = createModel('testings', { foo: String });
		model.index({ foo: 1, bar: 1 }, { unique: true });

		let collection;
		let promise = model.collectionPromise
			.then((_collection) => collection = _collection)
			.then(() => collection.insert({ foo: 'one', bar: 'two' }))
			.then((result) => collection.insert({ foo: 'one', bar: 'two' }))
			.catch((err) => {
				throw UnimongoError.fromMongoError(err, model);
			})
			.catch((err) => {
				expect(err).to.be.an.instanceOf(UnimongoError);
				expect(err.data.keys.length).to.equal(2);
				expect(err.data.keys[0]).to.equal('foo');
				expect(err.data.keys[1]).to.equal('bar');

				throw err;
			});

		return expect(promise).to.be.rejectedWith(UnimongoError);
	});

});


