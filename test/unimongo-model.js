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

	it('should construct the appropriate indices', function() {
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

});



