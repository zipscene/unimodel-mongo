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

	it('should retrieve keys', function() {
		let model = createModel('testings', {
			foo: String
		}, {
			keys: [ 'foo' ],
			initialize: false
		});

		let keys = model.getKeys();
		expect(keys.length).to.equal(1);
		expect(keys[0]).to.equal('foo');

		let model2 = createModel('testings', {
			foo: { type: String, key: true },
			bar: {
				baz: { type: Number, key: true }
			}
		}, { initialize: false });

		let keys2 = model2.getKeys();
		expect(keys2.length).to.equal(2);
		expect(keys2[0]).to.equal('foo');
		expect(keys2[1]).to.equal('bar.baz');

		let model3 = createModel('testings', {
			foo: String,
			bar: Number,
			baz: {
				oof: Boolean,
				rab: Date
			},
			buz: { type: 'geojson' }
		}, { initialize: false });
		model3.index({ foo: 1 });
		model3.index({ foo: 1, bar: -1 });
		model3.index({ foo: 1, bar: -1, 'baz.oof': 1 });
		model3.index({ foo: 1, bar: -1, 'baz': 1, buz: '2dsphere' });

		let keys3 = model3.getKeys();
		expect(keys3.length).to.equal(3);
		expect(keys3[0]).to.equal('baz.oof');
		expect(keys3[1]).to.equal('bar');
		expect(keys3[2]).to.equal('foo');

		let model4 = createModel('testings', {
			foo: { type: String, index: true },
			bar: { type: Number, unique: true },
			baz: {
				buz: [ { type: 'geopoint', index: true } ],
				bip: { type: 'geojson', index: '2dsphere' },
				zap: { type: String, index: true, sparse: true },
				zip: { type: Number, index: -1 }
			},
			bork: {
				fork: { type: String, index: true, sparse: true },
				dork: { type: Number, index: -1 }
			}
		}, { initialize: false });

		let keys4 = model4.getKeys();
		expect(keys4.length).to.equal(1);
		expect(keys4[0]).to.equal('foo');

		let model5 = createModel('testings', {
			foo: String,
			bar: {
				baz: { type: 'geojson', index: '2dsphere' }
			}
		}, { initialize: false });

		expect(() => model5.getKeys()).to.throw(Error);
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

	it('should update documents with UnimongoModel#update', function() {
		let model = createModel('testings', { foo: String });

		return model.insert({ foo: 'bar' })
			.then(() => model.update({ foo: 'bar' }, { foo: 'baz' }))
			.then((result) => {
				expect(result.result.nModified).to.equal(1);
			})
			.then(() => model.find({ foo: 'baz' }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal('baz');
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
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal(2);
				expect(documents[0]).to.be.an.instanceof(UnimongoDocument);
			});
	});

	it('should return documents from UnimongoModel#findStream', function() {
		let model = createModel('testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.findStream({ foo: 2 }).intoArray())
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal(2);
				expect(documents[0]).to.be.an.instanceof(UnimongoDocument);
			});
	});

	it('should handle partial documents in UnimongoModel#find', function() {
		let model = createModel('testings', { foo: Number, bar: Number });

		return model.insertMulti([ { foo: 1, bar: 1 }, { foo: 2, bar: 2 } ])
			.then(() => model.find({ foo: 2 }, { fields: [ 'bar' ] }))
			.then((documents) => {
				expect(documents[0].options.isPartial).to.be.true;
				expect(documents[0].data).to.deep.equal({ bar: 2 });
			});
	});

	it('should handle partial documents in UnimongoModel#findStream', function() {
		let model = createModel('testings', { foo: Number, bar: Number });

		return model.insertMulti([ { foo: 1, bar: 1 }, { foo: 2, bar: 2 } ])
			.then(() => model.findStream({ foo: 2 }, { fields: [ 'bar' ] }).intoArray())
			.then((documents) => {
				expect(documents[0].options.isPartial).to.be.true;
				expect(documents[0].data).to.deep.equal({ bar: 2 });
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

	it('should return number of matched records in UnimongoModel#count', function() {
		let model = createModel('testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 1 } ])
			.then(() => model.count({ foo: 1 }))
			.then((count) => {
				expect(count).to.equal(2);
			});
	});

	it('should remove records with UnimongoModel#remove', function() {
		let model = createModel('testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.remove({ foo: 1 }))
			.then(() => model.find({}))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal(2);
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
