const expect = require('chai').expect;
const {
	createModel,
	UnimongoDb,
	UnimongoError,
	UnimongoModel,
	UnimongoDocument
} = require('../lib');
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

	it('should recognize the appropriate indices', function() {
		let model = createModel('testings', {
			foo: { type: String, index: true },
			bar: { type: Number, unique: true },
			baz: {
				buz: [ { type: 'geopoint', index: true } ],
				bip: { type: 'geojson', index: '2dsphere' },
				zap: { type: String, index: true, sparse: true },
				zip: { type: Number, index: -1 }
			}
		}, {
			initialize: false
		});
		model.index({ foo: 1, bar: -1, 'baz.buz': '2dsphere' }, { unique: true, sparse: true });
		expect(model.getIndices()).to.deep.equal([
			{ spec: { foo: 1 }, options: {} },
			{ spec: { bar: 1 }, options: { unique: true } },
			{ spec: { 'baz.buz': '2dsphere' }, options: {} },
			{ spec: { 'baz.bip': '2dsphere' }, options: {} },
			{ spec: { 'baz.zap': 1 }, options: { sparse: true } },
			{ spec: { 'baz.zip': -1 }, options: {} },
			{ spec: { foo: 1, bar: -1, 'baz.buz': '2dsphere' }, options: { unique: true, sparse: true } }
		]);
	});

	it('should create indices', function() {
		let model = createModel('testings', {
			foo: { type: String, unique: true },
			bar: { type: 'geopoint', index: true }
		}, {
			autoIndexId: false
		});
		return model.collectionPromise
			.then((collection) => collection.indexes())
			.then((indexes) => {
				expect(indexes.length).to.equal(2);
			});
	});

	it('should not fail if indices already exist', function() {
		function makeModel() {
			return createModel('testings', {
				foo: { type: String, unique: true },
				bar: { type: 'geopoint', index: true }
			}, {
				autoIndexId: false
			});
		}
		return makeModel().collectionPromise
			.then(() => makeModel().collectionPromise)
			.then((collection) => collection.indexes())
			.then((indexes) => {
				expect(indexes.length).to.equal(2);
			});
	});

	it('should insert documents into the collection', function() {
		let model = createModel('testings', { foo: Number });

		return model
			.insert({ foo: '123' })
			.then((result) => {
				expect(result.data.foo).to.equal(123);
				expect(result.getInternalId()).to.exist;
			});
	});

	it('should insert multiple documents into the collection', function() {
		let model = createModel('testings', { foo: Number });

		return model
			.insertMulti([ { foo: '123' }, { foo: '234' } ])
			.then((results) => {
				expect(Array.isArray(results)).to.be.true;
				expect(results[0].data.foo).to.equal(123);
				expect(results[0].getInternalId()).to.exist;
				expect(results[1].data.foo).to.equal(234);
				expect(results[1].getInternalId()).to.exist;
			});
	});

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
			.then((document) => model.find({ foo: 'bar' }))
			.then((result) => {
				expect(result).to.be.empty;
			});
	});

	it('should return documents from UnimongoModel#find', function() {
		let model = createModel('testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.find({ foo: 2 }))
			.then((results) => {
				expect(results.length).to.equal(1);
				expect(results[0].data.foo).to.equal(2);
				expect(results[0]).to.be.an.instanceof(UnimongoDocument);
			});
	});

	it('should return documents from UnimongoModel#findStream', function() {
		let model = createModel('testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.findStream({ foo: 2 }).intoArray())
			.then((array) => {
				expect(array.length).to.equal(1);
				expect(array[0].data.foo).to.equal(2);
				expect(array[0]).to.be.an.instanceof(UnimongoDocument);
			});
	});

	it('should remove records with UnimongoModel#remove', function() {
		let model = createModel('testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.remove({ foo: 1 }))
			.then(() => model.find({}))
			.then((results) => {
				expect(results.length).to.equal(1);
				expect(results[0].data.foo).to.equal(2);
				expect(results[0]).to.be.an.instanceof(UnimongoDocument);
			});
	});

});
