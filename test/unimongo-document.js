const chai = require('chai');
const expect = chai.expect;
const { UnimongoDocument, UnimongoError, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');

chai.use(require('chai-as-promised'));

describe('UnimongoDocument', function() {

	beforeEach(testScaffold.resetAndConnect);

	it('should move internal id to the instance', function() {
		let model = createModel('testings', { foo: Number });

		return model
			.insert({ foo: '123' })
			.then((result) => {
				expect(result._id).to.exist;
				expect(result._originalId).to.exist;
				expect(result.getInternalId()).to.exist;
				expect(result.data._id).to.be.undefined;
				expect(result._originalData._id).to.be.undefined;
			});
	});

	it('should save changes to a new document', function() {
		let model = createModel('testings', { foo: String });

		let document = new UnimongoDocument(model, {
			foo: 'bar'
		});

		document.data.foo = 'baz';

		return document.save()
			.then((document) => {
				expect(document.data).to.deep.equal({ foo: 'baz' });
			});
	});

	it('should save changes to an existing document', function() {
		let model = createModel('testings', { foo: String });

		let document = new UnimongoDocument(model, {
			foo: 'bar'
		});

		return document.save()
			.then((document) => {
				document.data.foo = 'baz';

				return document.save();
			})
			.then((document) => {
				expect(document.data).to.deep.equal({ foo: 'baz' });
			});
	});

	it('should save changes to an existing document with a changed id', function() {
		let model = createModel('testings', { foo: String });

		let document = new UnimongoDocument(model, { foo: 'bar' });

		return document.save()
			.then((document) => {
				document.setInternalId('some-other-id');
				document.data.foo = 'baz';

				return document.save();
			})
			.then((document) => {
				expect(document.getInternalId()).to.equal('some-other-id');
			});
	});

	it('should error when updating document id to an existing one', function() {
		let model = createModel('testings', { foo: String });

		let document = new UnimongoDocument(model, { foo: 'bar' });
		let document2 = new UnimongoDocument(model, { foo: 'baz' });

		let existingId;
		return document.save()
			.then((document) => {
				existingId = document.getInternalId();
				return document2.save();
			})
			.then((document2) => {
				document2.setInternalId(existingId);

				return expect(document2.save()).to.be.rejectedWith(UnimongoError);
			});
	});

	it('should save changes to existing data with no document', function() {
		let model = createModel('testings', { foo: String });

		return model.insert({ foo: 'bar' })
			.then((document) => {
				document.data.foo = 'baz';
				return document.save();
			})
			.then((document) => {
				expect(document.data).to.deep.equal({ foo: 'baz' });
			});
	});

	it('should remove documents with UnimongoDocument#remove', function() {
		let model = createModel('testings', { foo: String });

		return model.insert({ foo: 'bar' })
			.then((document) => document.remove())
			.then(() => model.find({ foo: 'bar' }))
			.then((result) => {
				expect(result).to.be.empty;
			});
	});

	it('should properly handle saving partial documents in UnimongoDocument#save', function() {
		let model = createModel('testings', { foo: Number, bar: Number });
		let model2 = createModel('testings', { foo: Number, bar: Number }, { allowSavingPartials: false });

		return model.insert({ foo: 1, bar: 1 })
			.then(() => model.find({ foo: 1 }, { fields: [ 'bar' ] }))
			.then((documents) => {
				let document = documents[0];
				document.data.foo = 2;
				expect(() => document.save()).to.not.throw(Error);
			})
			.then(() => model2.find({ foo: 1 }, { fields: [ 'bar' ] }))
			.then((documents) => {
				let document = documents[0];
				document.data.foo = 2;
				expect(() => document.save()).to.throw(Error);
			});
	});

	it('should handle setting and removing items from the same array in UnimongoDocument#save', function() {
		let model = createModel('testings', { foo: [ String ] });

		let document = new UnimongoDocument(model, {
			foo: [ 'a', 'b', 'c' ]
		});

		return document.save()
			.then((document) => {
				document.data.foo = [ 'a', 'c' ];

				return document.save();
			})
			.then((document) => {
				expect(document.data.foo.length).to.equal(2);
				expect(document.data.foo[0]).to.equal('a');
				expect(document.data.foo[1]).to.equal('c');
			});
	});

});
