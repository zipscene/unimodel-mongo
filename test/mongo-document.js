// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const chai = require('chai');
const XError = require('xerror');
const { expect } = chai;
const { MongoDocument, MongoError, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');

chai.use(require('chai-as-promised'));

describe('MongoDocument', function() {
	beforeEach(testScaffold.resetAndConnect);

	it('should trigger post-init hooks on init', function() {
		this.timeout(100000);
		let docData = { biz: 'bar' };
		let model = createModel('testings', { biz: String });
		model.hook('post-init', function(doc) {
			expect(doc.getData()).to.deep.equal(docData);
			expect(this).to.equal(model);
			doc.getData().biz = 'baz';
		});
		let doc = model.create(docData);
		expect(doc.getData().biz).to.equal('baz');
		return model.collectionPromise;
	});

	it('should trigger pre-save hooks on init', function() {
		this.timeout(100000);
		let model = createModel('testings', { foo: String });
		let document = new MongoDocument(model, { foo: 'bar' });

		document.data.foo = 'baz';

		model.hook('pre-save', function(modelRef) {
			expect(document.data).to.deep.equal({ foo: 'baz' });
			modelRef.data.foo = 'zoo';
			expect(document.data).to.deep.equal({ foo: 'zoo' });
			expect(this).to.equal(model);
		});

		return document.save()
			.then((document) => {
				expect(document.getData().foo).to.equal('zoo');
			});
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

	it('should save changes that are reverted later with the same instance', function() {
		let model = createModel('testings', { foo: String });

		return Promise.resolve()
			.then(() => {
				let document = new MongoDocument(model, { foo: 'bar' });
				return document.save();
			})
			.then(() => {
				return model.findOne({});
			})
			.then((document) => {
				document.data.foo = 'baz';
				return document.save();
			})
			.then((document) => {
				document.data.foo = 'bar';
				return document.save();
			})
			.then(() => model.findOne({}))
			.then((newDocument) => {
				expect(newDocument.data).to.deep.equal({ foo: 'bar' });
			});
	});

	it('serialization should be invisible to user', function() {
		let model = createModel('test', { foo: {
			type: 'mixed',
			serializeMixed: true
		} });
		let document = new MongoDocument(model, { foo: { $bar: 'baz' } });

		return document.save()
			.then((document) => {
				document.data.foo = { $bar: 'lol' };
				return document.save();
			})
			.then((document) => {
				// console.log(document.data);
				expect(document.data).to.deep.equal({ foo: { $bar: 'lol' } });
			});

	});

	it('should update revision number on existing document', function() {
		let model = createModel('testings', { foo: String });
		let document = new MongoDocument(model, { foo: 'bar' });
		let revisionNumber = document._revisionNumber;

		return document.save()
			.then((document) => {
				document.data.foo = 'baz';
				return document.save();
			})
			.then((document) => {
				expect(++revisionNumber).to.equal(document._revisionNumber);
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

		let error1, error2;
		return Promise.resolve()
			.then(() => model.insert({ foo: 1, bar: 1 }))
			.then(() => model.find({ foo: 1 }, { fields: [ 'bar' ] }))
			.then((documents) => {
				let document = documents[0];
				document.data.foo = 2;

				return document.save()
					.catch((err) => {
						error1 = err;
					});
			})
			.then(() => {
				expect(error1).to.be.undefined;
			})
			.then(() => model2.insert({ foo: 1, bar: 1 }))
			.then(() => model2.find({ foo: 1 }, { fields: [ 'bar' ] }))
			.then((documents) => {
				let document = documents[0];
				document.data.foo = 2;

				return document.save()
					.catch((err) => {
						error2 = err;
					});
			})
			.then(() => {
				expect(error2).to.be.an.instanceof(XError);
				expect(error2.code).to.equal(XError.UNSUPPORTED_OPERATION);
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
