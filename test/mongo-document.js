const chai = require('chai');
const expect = chai.expect;
const { MongoDocument, MongoError, createModel } = require('../lib');
const Model = require('zs-unimodel').Model;
const testScaffold = require('./lib/mongo-scaffold');
const bson = require('bson');
const BSON = new bson.BSONPure.BSON();
const { map } = require('zs-common-schema');

chai.use(require('chai-as-promised'));

describe('MongoDocument', function() {
	beforeEach(testScaffold.resetAndConnect);

	it('should trigger post-init hooks on init', function() {
		class TestDocument extends MongoDocument {
			constructor(model, data) { super(model, data); }
		}

		class TestModel extends Model {
			create(data) { return new TestDocument(this, data); }
		}

		const testModel = new TestModel();
		const docData = { foo: 'bar' };

		testModel.hook('post-init', function(doc) {
			expect(doc.getData()).to.deep.equal(docData);
			expect(this).to.equal(testModel);
			doc.getData().biz = 'baz';
		});

		const doc = testModel.create(docData);
		expect(doc.getData().biz).to.equal('baz');
	});

	it('should move internal id to the instance', function() {
		let model = createModel('testings', { foo: Number });

		return model.insert({ foo: '123' })
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
		let document = new MongoDocument(model, { foo: 'bar' });

		document.data.foo = 'baz';

		return document.save()
			.then((document) => {
				expect(document.data).to.deep.equal({ foo: 'baz' });
			});
	});

	it('should save changes to an existing document', function() {
		let model = createModel('testings', { foo: String });
		let document = new MongoDocument(model, { foo: 'bar' });

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
		let document = new MongoDocument(model, { foo: 'bar' });

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
		let document = new MongoDocument(model, { foo: 'bar' });
		let document2 = new MongoDocument(model, { foo: 'baz' });

		let existingId;

		return model.collectionPromise
			.then(() => {
				return document.save()
					.then((document) => {
						existingId = document.getInternalId();
						return document2.save();
					})
					.then((document2) => {
						document2.setInternalId(existingId);

						return expect(document2.save()).to.be.rejectedWith(MongoError);
					});
			});
	});

	it('should save changes to existing data with no document', function() {
		let model = createModel('testings', { foo: String, bar: String });

		return model.insert({ foo: 'a', bar: 'b' })
			.then((document) => {
				document.data.foo = 'c';

				return document.save();
			})
			.then((document) => {
				expect(document.data).to.deep.equal({ foo: 'c', bar: 'b' });
			});
	});

	it('should remove documents with MongoDocument#remove', function() {
		let model = createModel('testings', { foo: String });

		return model.insert({ foo: 'bar' })
			.then((document) => document.remove())
			.then(() => model.find({ foo: 'bar' }))
			.then((result) => {
				expect(result).to.be.empty;
			});
	});

	it('should properly handle saving partial documents in MongoDocument#save', function() {
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

	it('should handle setting and removing items from the same array in MongoDocument#save', function() {
		let model = createModel('testings', { foo: [ String ] });
		let document = new MongoDocument(model, { foo: [ 'a', 'b', 'c' ] });

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

	it('should properly increment revision number when saving documents', function() {
		let model = createModel('testings', { foo: String });
		let model3 = createModel('testings', { foo: [ String ] });
		let document1 = new MongoDocument(model, { foo: 'bar' });
		let document2 = new MongoDocument(model, { foo: 'bar' });
		let document3 = new MongoDocument(model3, { foo: [ 'a', 'b', 'c' ] });

		document1.data.foo = 'baz';

		expect(document1._revisionNumber).to.equal(1);
		expect(document2._revisionNumber).to.equal(1);
		expect(document3._revisionNumber).to.equal(1);

		return model.insert({ foo: 'bar' })
			.then((document) => {
				document.data.foo = 'baz';

				expect(document._revisionNumber).to.be.undefined;

				return document.save();
			})
			.then((document) => {
				expect(document._revisionNumber).to.equal(1);
			})
			.then(() => document1.save())
			.then((document1) => {
				expect(document1._revisionNumber).to.equal(1);
			})
			.then(() => document2.save())
			.then((document2) => {
				document2.data.foo = 'baz';

				expect(document2._revisionNumber).to.equal(1);

				return document2.save();
			})
			.then((document2) => {
				expect(document2._revisionNumber).to.equal(2);
			})
			.then(() => document3.save())
			.then((document3) => {
				document3.data.foo = [ 'a', 'c' ];

				expect(document3._revisionNumber).to.equal(1);

				return document3.save();
			})
			.then((document3) => {
				expect(document3._revisionNumber).to.equal(3);
			});
	});

	it('should clean up instance when saving documents', function() {
		let model = createModel('testings', { foo: String });
		let document = new MongoDocument(model, { foo: 'bar' });

		return document.save()
			.then((document) => {
				expect(document.data._id).to.be.undefined;
				expect(document.data.__rev).to.be.undefined;
			});
	});

});
