// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const _ = require('lodash');
const chai = require('chai');
const XError = require('xerror');
const { expect } = chai;
const { MongoDocument, MongoModel, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');
const { map } = require('common-schema');
const { createQuery, createUpdate } = require('common-query');
const sinon = require('sinon');

chai.use(require('chai-as-promised'));

const keySort = (a, b) => {
	for (let key in a.key) {
		if (''+a.key[key] > ''+b.key[key] || typeof b.key[key] === 'undefined') return 1;
		if (''+a.key[key] < ''+b.key[key]) return -1;
	}
	return 0;
};

describe('MongoModel', function() {
	beforeEach(testScaffold.resetAndConnect);

	it('#getName', function() {
		let model = createModel('FooBar', { foo: String });
		return model.collectionPromise
			.then(() => {
				expect(model.getName()).to.equal('FooBar');
			});
	});

	it('#getCollectionName', function() {
		let model = createModel('FooBar', { foo: String });
		return model.collectionPromise
			.then(() => {
				expect(model.getCollectionName()).to.equal('fooBar');
			});
	});

	it('should create the collection when it doesnt exist', function() {
		let model = createModel('Testings', { foo: String });
		return model.collectionPromise;
	});

	it('should not fail if the collection already exists', function() {
		let model1 = createModel('Testings', { foo: String });
		return model1.collectionPromise
			.then(() => {
				let model2 = createModel('Testings', { foo: String });
				return model2.collectionPromise;
			});
	});

	it('should recognize the appropriate indexes', function() {
		let model = createModel('Testings', {
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
		expect(model.getIndexes()).to.deep.equal([
			{ spec: { foo: 1 }, options: {} },
			{ spec: { bar: 1 }, options: { unique: true } },
			{ spec: { 'baz.buz': '2dsphere' }, options: {} },
			{ spec: { 'baz.bip': '2dsphere' }, options: {} },
			{ spec: { 'baz.zap': 1 }, options: { sparse: true } },
			{ spec: { 'baz.zip': -1 }, options: {} },
			{ spec: { foo: 1, bar: -1, 'baz.buz': '2dsphere' }, options: { unique: true, sparse: true } }
		]);
	});

	it('should convert nested geopoint indexes to 2dsphere', function() {
		let model = createModel('Testings', {
			foo: { type: 'geopoint', index: true },
			bar: [ { type: 'geopoint', index: true } ],
			baz: {
				barn: { type: 'geopoint', index: true },
				bark: [ { type: 'geopoint', index: true } ]
			}
		}, { initialize: false });

		expect(model.getIndexes()).to.deep.equal([
			{ spec: { foo: '2dsphere' }, options: {} },
			{ spec: { bar: '2dsphere' }, options: {} },
			{ spec: { 'baz.barn': '2dsphere' }, options: {} },
			{ spec: { 'baz.bark': '2dsphere' }, options: {} }
		]);
	});

	it('should support compound indexes', function() {
		let model = createModel('Testings', {
			foo: { type: String, index: { foo: 1, bar: 1 } },
			bar: { type: Number, index: { bar: 1, foo: -1 } },
			baz: {
				zap: { type: String, index: { zap: 1, zip: 1 }, sparse: true },
				zip: { type: Number, index: { zip: 1, zap: -1 } }
			}
		}, {
			initialize: false
		});
		expect(model.getIndexes()).to.deep.equal([
			{ spec: { foo: 1, bar: 1 }, options: {} },
			{ spec: { foo: -1, bar: 1 }, options: {} },
			{ spec: { 'baz.zap': 1, 'baz.zip': 1 }, options: { sparse: true } },
			{ spec: { 'baz.zip': 1, 'baz.zap': -1 }, options: {} }
		]);
	});

	it('should remove redundant indexes', function() {
		let model = createModel('Testings', {
			foo: String,
			bar: Number,
			baz: Boolean,
			quux: String
		}, {
			initialize: false
		});
		model.index({ foo: 1 });
		model.index({ bar: 1 });
		model.index({ foo: 1, bar: 1 });
		model.index({ foo: 1, bar: 1, baz: 1 }, { someOption: true });
		model.index({ foo: 1, bar: 1, baz: 1, quux: 1 }, { someOption: true });
		model.index({ foo: 1, bar: 1, baz: 1, quux: -1 }, { someOption: true });
		// indexes are deduplicated on initCollection
		let indexes =  model._removeRedundantIndexes();
		expect(indexes).to.deep.equal([
			{ spec: { bar: 1 }, options: {} },
			{ spec: { foo: 1, bar: 1 }, options: {} },
			{ spec: { foo: 1, bar: 1, baz: 1, quux: 1 }, options: { someOption: true } },
			{ spec: { foo: 1, bar: 1, baz: 1, quux: -1 }, options: { someOption: true } }
		]);
	});

	it('should throw an error on incorrectly-ordered indexes', function() {
		let fn1 = () => {
			createModel('A', {
				foo: { type: String, index: { foo: 1, bar: 1 } },
				bar: String
			}, { initialize: false });
		};

		let fn2 = () => {
			createModel('B', {
				foo: { type: String, index: { bar: 1, foo: 1 } },
				bar: String
			}, { initialize: false });
		};

		let fn3 = () => {
			createModel('C', {
				baz: {
					foo: { type: String, index: { foo: 1, bar: 1 } },
					bar: String
				}
			}, { initialize: false });
		};

		let fn4 = () => {
			createModel('D', {
				baz: {
					foo: { type: String, index: { bar: 1, foo: 1 } },
					bar: String
				}
			}, { initialize: false });
		};

		expect(fn1).to.not.throw(XError);
		expect(fn2).to.throw(XError);
		expect(fn3).to.not.throw(XError);
		expect(fn4).to.throw(XError);
	});

	it('should retrieve keys', function() {
		let model = createModel('Testings', {
			foo: String
		}, {
			keys: [ 'foo' ],
			initialize: false
		});

		let keys = model.getKeys();
		expect(keys.length).to.equal(1);
		expect(keys[0]).to.equal('foo');

		let model2 = createModel('Testings', {
			foo: { type: String, key: true },
			bar: {
				baz: { type: Number, key: true }
			}
		}, { initialize: false });

		let keys2 = model2.getKeys();
		expect(keys2.length).to.equal(2);
		expect(keys2[0]).to.equal('foo');
		expect(keys2[1]).to.equal('bar.baz');

		let model3 = createModel('Testings', {
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

		let model4 = createModel('Testings', {
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

		let model5 = createModel('Testings', {
			foo: String,
			bar: {
				baz: { type: 'geojson', index: '2dsphere' }
			}
		}, { initialize: false });

		expect(() => model5.getKeys()).to.throw(Error);
	});

	it('should create indexes by default', function() {
		let model = createModel('Testings', {
			foo: { type: String, unique: true },
			bar: { type: 'geopoint', index: true }
		});

		return model.collectionPromise
			.then((collection) => collection.indexes())
			.then((indexes) => {
				expect(indexes.length).to.equal(3);
			});
	});

	it('should not create indices on initialization if options.autoCreateIndex is set to false', function() {
		return testScaffold.close()
			.then(() => testScaffold.connect({ autoCreateIndex: false }) )
			.then(() => {
				let model = createModel('Testings', {
					foo: { type: String, unique: true },
					bar: { type: 'geopoint', index: true }
				});

				return model.collectionPromise
					.then((collection) => collection.indexes())
					.then((indexes) => {
						expect(indexes.length).to.equal(1);
					});
			});
	});

	it('should create indices when calling ensureIndexes', function() {
		return testScaffold.close()
			.then(() => testScaffold.connect({ autoCreateIndex: false }))
			.then(() => {
				let model = createModel('Testings', {
					foo: { type: String, unique: true },
					bar: { type: 'geopoint', index: true }
				}, {
					autoCreateIndex: false
				});

				return model.ensureIndexes()
					.then(() => {
						expect(model.indexes).to.have.length(3);
					});
			});
	});

	/* Helper function for the following tests.
	 * Asserts that the indexes on a MongoModel's collection match those specified in its schema.
	 * Only specs are checked for each index, not options. */
	function checkIndexes(model) {
		function hashByKeys(obj) {
			return Object.keys(obj).sort().join('.');
		}

		function compareByKeys(a, b) {
			return hashByKeys(a) > hashByKeys(b);
		}

		return model.collectionPromise
			.then((collection) => collection.indexes())
			.then((collectionIndexes) => {
				// Ensure indexes property is up to date.
				expect(model.indexes).to.deep.equal(collectionIndexes);

				// Specs must be sorted, otherwise the order may differ.
				let collectionSpecs = _.pluck(model.indexes, 'key').sort(compareByKeys);
				let schemaSpecs = _.pluck(model.getIndexes(), 'spec').sort(compareByKeys);

				// Mongo mandates the `_id` index as of 3.4.
				schemaSpecs.unshift({ _id: 1 });

				// Ensure specs in indexes property match specs in schema.
				expect(collectionSpecs).to.deep.equal(schemaSpecs);
			});
	}

	it('should remove indexes that are not in the schema when calling removeIndexes', function() {
		return testScaffold.close()
			.then(() => testScaffold.connect({ autoCreateIndex: false }))
			.then(() => {
				let model1 = createModel('Testings', {
					foo: { type: String, unique: true },
					bar: { type: 'geopoint', index: true },
					baz: { type: String, index: true }
				});

				let model2 = createModel('Testings', {
					foo: { type: String, unique: true },
					bar: { type: 'geopoint', index: true }
				});

				return model1.ensureIndexes()
					.then(() => checkIndexes(model1))
					.then(() => model2.removeIndexes())
					.then(() => checkIndexes(model2));
			});
	});

	it('should be sensitive to order of spec keys when calling removeIndexes', function() {
		return testScaffold.close()
			.then(() => testScaffold.connect({ autoCreateIndex: true }))
			.then(() => {
				let model = createModel('Testings', {
					foo: { type: String, unique: true },
					bar: { type: 'geopoint', index: true }
				});

				model.index({ foo: 1, bar: 1 });

				return checkIndexes(model)
					.then(() => model.collectionPromise)
					.then((collection) => collection.createIndex({ bar: 1, foo: 1 }))
					.then(() => model.removeIndexes())
					.then(() => checkIndexes(model));
			});
	});

	it('should synchronize indexes with schema when calling synchronizeIndexes', function() {
		return testScaffold.close()
			.then(() => testScaffold.connect({ autoCreateIndex: false }))
			.then(() => {
				let model1 = createModel('Testings', {
					foo: { type: String, unique: true }
				});

				let model2 = createModel('Testings', {
					bar: { type: 'geopoint', index: true },
					baz: { type: String, index: true }
				});

				return model1.ensureIndexes()
					.then(() => checkIndexes(model1))
					.then(() => model2.synchronizeIndexes())
					.then(() => checkIndexes(model2));
			});
	});

	it('should create indices in background', function() {
		return testScaffold.close()
			.then(() => testScaffold.connect({
				autoCreateIndex: true,
				backgroundIndex: true
			}))
			.then(() => {
				let model = createModel('Testings', {
					foo: { type: String, unique: true },
					bar: { type: 'geopoint', index: true }
				}, {
					backgroundIndex: true
				});

				model.index({ foo: 1, bar: 1 });

				return model.collectionPromise
					.then(() => {
						expect(model.indexes).to.have.length(4);
						for (let index of model.indexes) {
							if (index.name === '_id_') continue;
							expect(index).to.have.property('background', true);
						}
					});
			});
	});

	it('should not fail if indices already exist', function() {
		function makeModel() {
			return createModel('Testings', {
				foo: { type: String, unique: true },
				bar: { type: 'geopoint', index: true }
			});
		}

		let model1 = makeModel();
		let model2 = makeModel();

		return model1.collectionPromise
			.then(() => model2.collectionPromise)
			.then((collection) => collection.indexes())
			.then((indexes) => {
				expect(indexes.length).to.equal(3);
			});
	});

	it('should flag existing documents with `isExisting`', function() {
		let model = createModel('Testings', { foo: Number });

		return model.insert({ foo: '123' })
			.then((document) => {
				expect(document.options.isExisting).to.be.true;
				expect(document.getInternalId()).to.exist;
			});
	});

	it('should insert documents into the collection', function() {
		let model = createModel('Testings', { foo: Number });

		return model.insert({ foo: '123' })
			.then((document) => {
				expect(document.data.foo).to.equal(123);
				expect(document.getInternalId()).to.exist;
			});
	});

	it('should insert multiple documents into the collection', function() {
		let model = createModel('Testings', { foo: Number });

		return model.insertMulti([ { foo: '123' }, { foo: '234' } ])
			.then((results) => {
				expect(Array.isArray(results)).to.be.true;
				expect(results[0].data.foo).to.equal(123);
				expect(results[0].getInternalId()).to.exist;
				expect(results[1].data.foo).to.equal(234);
				expect(results[1].getInternalId()).to.exist;
			});
	});

	it('MongoModel#update should update documents', function() {
		let model = createModel('Testings', { foo: String });

		return model.insert({ foo: 'bar' })
			.then(() => model.update({ foo: 'bar' }, { foo: 'baz' }))
			.then((numUpdated) => {
				expect(numUpdated).to.equal(1);
			})
			.then(() => model.find({ foo: 'baz' }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal('baz');
			});
	});

	it('MongoModel#update should update map docuemnts using a stream', function() {
		let model = createModel('Testings', { foo: map({}, String) });

		return model.insert({ foo: { lol: 'bar' } })
			.then(() => model.update({ foo: { lol: 'bar' } }, { foo: { lol: 'baz' } }))
			.then((numUpdated) => {
				expect(numUpdated).to.equal(1);
			})
			.then(() => model.find({ foo: { lol: 'baz' } }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo.lol).to.equal('baz');
			});
	});

	it('MongoModel#update should serialize mixed when doing full replacement', function() {
		let model = createModel('Testings', {
			foo: {
				type: 'mixed',
				serializeMixed: true
			},
			bar: Number
		});
		let update = createUpdate({ foo: { $lol: 'baz' }, bar: 1 }, { allowFullReplace: true });

		return model.insert({ foo: { $lol: 'bar' }, bar: 1 })
			.then(() => model.update({ bar: 1 }, update))
			.then((numUpdated) => {
				expect(numUpdated).to.equal(1);
			})
			.then(() => model.find({ bar: 1 }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo.$lol).to.equal('baz');
			});
	});

	it('MongoModel#update should serialize mixed when updating only the field', function() {
		let model = createModel('Testings', {
			foo: {
				type: 'mixed',
				serializeMixed: true
			},
			bar: Number
		});

		return model.insert({ foo: { $lol: 'bar' }, bar: 1 })
			.then(() => model.update({ bar: 1 }, { $set: { foo: { $lol: 'baz' } } }))
			.then((numUpdated) => {
				expect(numUpdated).to.equal(1);
			})
			.then(() => model.find({ bar: 1 }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo.$lol).to.equal('baz');
			});
	});

	it('MongoModel#upsert should create a document if none match the query', function() {
		let model = createModel('Testings', { foo: String });

		return model.upsert({ foo: 'bar' }, { foo: 'baz' })
			.then((doc) => {
				expect(doc.data.foo).to.equal('baz');
			})
			.then(() => model.find({ foo: 'baz' }))
			.then((documents) => {
				expect(documents).to.have.length(1);
				expect(documents[0].data.foo).to.equal('baz');
			});
	});

	it('MongoModel#upsert should update documents if some are found', function() {
		let model = createModel('Testings', { foo: String });

		return model.insert({ foo: 'bar' })
			.then(() => model.upsert({ foo: 'bar' }, { foo: 'baz' }))
			.then(() => model.find({ foo: 'baz' }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal('baz');
			});
	});

	it('should return documents from MongoModel#find', function() {
		let model = createModel('Testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.find({ foo: 2 }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal(2);
				expect(documents[0]).to.be.an.instanceof(MongoDocument);
			});
	});

	it('should return sorted by distance documents from MongoModel#find when using a $near query', function() {
		let model = createModel('Testings', { point: { type: 'geopoint', index: true } });
		let records = [];
		let coordinates = [ 84, 39 ];
		records.push({ point: coordinates });
		for (let r = 0; r < 4; r++) {
			let xPoint = parseInt(Math.random()*100);
			let yPoint = Math.random();
			if (yPoint >= 0.9) {
				yPoint = yPoint*10;
			} else {
				yPoint = yPoint*100;
			}
			yPoint = parseInt(yPoint);
			records.push({ point: [ xPoint, yPoint ] });
		}
		let query = { point: { $near: { $geometry: { type: 'Point', coordinates } } } };

		return model.collectionPromise
			.then(() => model.insertMulti(records) )
			.then(() => model.find(query))
			.then((documents) => {
				expect(documents.length).to.equal(5);
				let commonQuery = createQuery(query);
				let previousDistance = 0;
				for (let document of documents) {
					commonQuery.matches(document.data);
					let currentDistance = commonQuery.getMatchProperty('distance');
					expect(currentDistance).to.be.at.least(previousDistance);
					previousDistance = currentDistance;
				}
			});
	});

	it('should support cursor options in MongoModel#find', function() {
		let model = createModel('Testings', { foo: Number, bar: Boolean, baz: Boolean });

		let records = [];
		for (let r = 0; r < 100; r++) {
			records.push({ foo: r, bar: true, baz: true });
		}

		return model.insertMulti(records)
			.then(() => model.find({}, {
				skip: 8,
				limit: 32,
				fields: [ 'foo', 'bar' ],
				total: true,
				sort: [ 'bar', '-foo' ],
				canCursorTimeout: false
			}))
			.then((documents) => {
				expect(documents.length).to.equal(32);
				expect(documents.total).to.equal(100);

				let keys = Object.keys(documents[0].data);
				expect(documents[0].data.foo).to.equal(91);
				expect(keys.length).to.equal(2);
				expect(keys[0]).to.equal('foo');
				expect(keys[1]).to.equal('bar');
			});
	});

	it('should return documents from MongoModel#findStream', function() {
		let model = createModel('Testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.findStream({ foo: 2 }).intoArray())
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal(2);
				expect(documents[0]).to.be.an.instanceof(MongoDocument);
			});
	});

	it('should support cursor options in MongoModel#findStream', function() {
		let model = createModel('Testings', { foo: Number, bar: Boolean, baz: Boolean });

		let records = [];
		for (let r = 0; r < 100; r++) {
			records.push({ foo: r, bar: true, baz: true });
		}

		return model.insertMulti(records)
			.then(() => model.findStream({}, {
				skip: 8,
				limit: 32,
				fields: [ 'foo', 'bar' ],
				total: true,
				sort: [ 'bar', '-foo' ],
				canCursorTimeout: false
			}).intoArray())
			.then((documents) => {
				expect(documents.length).to.equal(32);

				let keys = Object.keys(documents[0].data);
				expect(documents[0].data.foo).to.equal(91);
				expect(keys.length).to.equal(2);
				expect(keys[0]).to.equal('foo');
				expect(keys[1]).to.equal('bar');
			});
	});

	it('should handle partial documents in MongoModel#find', function() {
		let model = createModel('Testings', { foo: Number, bar: Number });

		return model.insertMulti([ { foo: 1, bar: 1 }, { foo: 2, bar: 2 } ])
			.then(() => model.find({ foo: 2 }, { fields: [ 'bar' ] }))
			.then((documents) => {
				expect(documents[0].options.isPartial).to.be.true;
				expect(documents[0].data).to.deep.equal({ bar: 2 });
				expect(documents[0].fields).to.deep.equal([ 'bar', '__rev' ]);
			});
	});

	it('should handle partial documents in MongoModel#findStream', function() {
		let model = createModel('Testings', { foo: Number, bar: Number });

		return model.insertMulti([ { foo: 1, bar: 1 }, { foo: 2, bar: 2 } ])
			.then(() => model.findStream({ foo: 2 }, { fields: [ 'bar' ] }).intoArray())
			.then((documents) => {
				expect(documents[0].options.isPartial).to.be.true;
				expect(documents[0].data).to.deep.equal({ bar: 2 });
			});
	});

	it('should handle hints in MongoModel#find', function() {
		let model = createModel('Testings', {
			foo: { type: Number, index: true },
			bar: { type: Number, index: true }
		});
		return model.insertMulti([ { foo: 1, bar: 1 }, { foo: 2, bar: 2 } ])
			.then(() => model.find({ foo: 2 }, { hint: { bar: 1 } }))
			.then((documents) => {
				expect(documents.length).to.equal(1);
			});
	});

	it('should return number of matched records in MongoModel#count', function() {
		let model = createModel('Testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 1 } ])
			.then(() => model.count({ foo: 1 }))
			.then((count) => {
				expect(count).to.equal(2);
			});
	});

	it('should remove records with MongoModel#remove', function() {
		let model = createModel('Testings', { foo: Number });

		return model.insertMulti([ { foo: 1 }, { foo: 2 } ])
			.then(() => model.remove({ foo: 1 }))
			.then(() => model.find({}))
			.then((documents) => {
				expect(documents.length).to.equal(1);
				expect(documents[0].data.foo).to.equal(2);
			});
	});

	describe('aggregates', function() {
		let hasFacetSupport;

		beforeEach(function() {
			hasFacetSupport = sinon.stub(MongoModel.prototype, '_hasFacetSupport');
		});

		afterEach(function() {
			hasFacetSupport.restore();
		});

		context('facet support', function() {
			beforeEach(function() {
				hasFacetSupport.returns(true);
			});

			testAggregates();
		});

		context('no facet support', function() {
			beforeEach(function() {
				hasFacetSupport.returns(false);
			});

			testAggregates();
		});

		function testAggregates() {
			it('should run aggregates on nested fields', function() {
				let model = createModel('Testings', {
					foo: { bar: { baz: Number } }
				});
				return model.insertMulti([
					{ foo: { bar: { baz: 1 } } },
					{ foo: { bar: { baz: 2 } } },
					{ foo: { bar: { baz: 3 } } }
				])
					.then(() => {
						return model.aggregate({}, {
							stats: {
								'foo.bar.baz': {
									count: true,
									avg: true
								}
							},
							total: true
						});
					})
					.then((result) => {
						let expected = { stats: { 'foo.bar.baz': { count: 3, avg: 2 } }, total: 3 };
						expect(result).to.deep.equal(expected);
					});
			});

			it('should run aggregates on nested array fields', function() {
				let model = createModel('Testings', {
					foo: [ {
						bar: [ {
							baz: Number
						} ]
					} ]
				});
				return model.insertMulti([
					{ foo: [ { bar: [ { baz: 1 } ] } ] },
					{ foo: [ { bar: [ { baz: 2 } ] } ] },
					{ foo: [ { bar: [ { baz: 3 } ] } ] }
				])
					.then(() => {
						return model.aggregate({}, {
							stats: {
								'foo.bar.baz': {
									count: true,
									avg: true,
									max: true,
									min: true
								}
							},
							total: true
						});
					})
					.then((result) => {
						let expected = { stats: { 'foo.bar.baz': { count: 3, avg: 2, max: 3, min: 1 } }, total: 3 };
						expect(result).to.deep.equal(expected);
					});
			});

			it('should run group aggregates on nested array fields', function() {
				let model = createModel('Testings', {
					foo: [ {
						bar: [ {
							baz: Number
						} ]
					} ],
					buz: Number
				});
				return model.insertMulti([
					{ foo: [ { bar: [ { baz: 3 } ] } ], buz: 2 },
					{ foo: [ { bar: [ { baz: 2 } ] } ], buz: 1 },
					{ foo: [ { bar: [ { baz: 3 } ] } ], buz: 3 }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'foo.bar.baz' } ],
							stats: {
								buz: {
									avg: true
								}
							},
							total: true
						});
					})
					.then((result) => {
						expect(result).to.be.an('array');
						expect(result).to.have.length(2);
						expect(result).to.deep.equal([
							{ stats: { buz: { avg: 1 } }, total: 1, key: [ 2 ] },
							{ stats: { buz: { avg: 2.5 } }, total: 2, key: [ 3 ] }
						]);
					});
			});

			it('should obey the \'only\' aggregate modifier', function() {
				let model = createModel('Testings', {
					foo: [ String ],
					buz: Number
				});
				return model.insertMulti([
					{ foo: [ 'a' ], buz: 1 },
					{ foo: [ 'b' ], buz: 1 },
					{ foo: [ 'c' ], buz: 1 },
					{ foo: [ 'a', 'b', 'c' ], buz: 1 }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ {
								field: 'foo',
								only: [ 'a', 'c' ]
							} ],
							stats: {
								buz: {
									sum: 1
								}
							},
							total: true
						});
					})
					.then((result) => {
						expect(result).to.be.an('array');
						expect(result).to.have.length(2);
						expect(result).to.deep.equal([
							{ stats: { buz: { sum: 2 } }, total: 2, key: [ 'a' ] },
							{ stats: { buz: { sum: 2 } }, total: 2, key: [ 'c' ] }
						]);
					});
			});

			it('should run grouped aggregates on nested fields', function() {
				let model = createModel('Testings', {
					biz: { buz: String },
					foo: {
						bar: {
							baz: Number
						}
					}
				});
				return model.insertMulti([
					{ biz: { buz: 'red' }, foo: { bar: { baz: 1 } } },
					{ biz: { buz: 'red' }, foo: { bar: { baz: 2 } } },
					{ biz: { buz: 'blue' }, foo: { bar: { baz: 3 } } }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: 'biz.buz',
							stats: {
								'foo.bar.baz': {
									count: true,
									avg: true
								}
							},
							total: true
						});
					})
					.then((result) => {
						let expected = [
							{
								stats: {
									'foo.bar.baz': {
										count: 1,
										avg: 3
									}
								},
								total: 1,
								key: [ 'blue' ]
							},
							{
								stats: {
									'foo.bar.baz': {
										count: 2,
										avg: 1.5
									}
								},
								total: 2,
								key: [ 'red' ]
							}
						];
						expect(result).to.deep.equal(expected);
					});
			});

			it('should run grouped and ungrouped aggregates with stats', function() {
				let model = createModel('Testings', {
					foo: String,
					bar: String,
					baz: Number,
					qux: Date
				});

				return model.insertMulti([
					{ foo: 'a', bar: 'x', baz: 1, qux: new Date('2000') },
					{ foo: 'a', bar: 'y', baz: 2, qux: new Date('2000') },
					{ foo: 'a', bar: 'y', baz: 3, qux: new Date('2010') },
					{ foo: 'b', bar: 'x', baz: 4, qux: new Date('2010') },
					{ foo: 'b', bar: 'y', baz: 5, qux: new Date('2020') },
					{ foo: 'b', bar: 'y', baz: 6, qux: new Date('2020') },
					{ foo: 'b', baz: 6 }
				])
					.then(() => {
						return model.aggregate({
							foo: 'a'
						}, {
							stats: {
								baz: { count: true, avg: true, min: true, max: true },
								qux: { min: true, max: true }
							},
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal({
							stats: {
								baz: { count: 3, avg: 2, min: 1, max: 3 },
								qux: { min: new Date('2000'), max: new Date('2010') }
							},
							total: 3
						});
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: 'foo',
							total: true
						});
					})
					.then((result) => {
						let expected = [
							{ key: [ 'a' ], total: 3 },
							{ key: [ 'b' ], total: 4 }
						].sort(keySort);
						expect(result).to.deep.equal(expected);
					})
					.then(() => {
						return model.aggregateMulti({}, {
							one: {
								groupBy: [ { field: 'foo' } ]
							},
							two: {
								stats: {
									foo: { count: true },
									bar: { count: true },
									baz: { count: true, avg: true }
								}
							},
							three: {
								groupBy: 'bar',
								stats: {
									foo: { count: true, min: true, max: true }
								},
								total: true
							}
						});
					})
					.then((result) => {
						let expected = {
							one: [
								{ key: [ 'a' ] },
								{ key: [ 'b' ] }
							],
							two: {
								stats: {
									foo: { count: 7 },
									bar: { count: 6 },
									baz: { count: 7, avg: 27 / 7 }
								}
							},
							three: [ {
								key: [ null ],
								stats: {
									foo: { count: 1, min: 'b', max: 'b' }
								},
								total: 1
							}, {
								key: [ 'x' ],
								stats: {
									foo: { count: 2, min: 'a', max: 'b' }
								},
								total: 2
							}, {
								key: [ 'y' ],
								stats: {
									foo: { count: 4, min: 'a', max: 'b' }
								},
								total: 4
							} ]
						};
						expect(result).to.deep.equal(expected);
					});
			});

			it('should support grouping by boolean fields', function() {
				let model = createModel('Testings', { foo: { bar: Boolean } });
				return model.insertMulti([ { foo: { bar: true } }, { foo: { bar: true } }, { foo: { bar: false } } ])
					.then(() => model.aggregate({}, { groupBy: 'foo.bar', total: true }))
					.then((result) => {
						let expected = [ { key: [ false ], total: 1 }, { key: [ true ], total: 2 } ];
						expect(result).to.deep.equal(expected);
					});
			});

			it('should support grouping by numeric fields', function() {
				let model = createModel('Testings', { foo: { bar: Number } });
				return model.insertMulti([ { foo: { bar: 0 } }, { foo: { bar: 1 } }, { foo: { bar: 1 } } ])
					.then(() => model.aggregate({}, { groupBy: 'foo.bar', total: true }))
					.then((result) => {
						let expected = [ { key: [ 0 ], total: 1 }, { key: [ 1 ], total: 2 } ];
						expect(result).to.deep.equal(expected);
					});
			});

			it('should support grouping by array index fields', function() {
				let model = createModel('Testings', { foo: [ Number ] });
				return model.insertMulti([ { foo: [ 1, 2 ] }, { foo: [ 5, 2 ] } ])
					.then(() => model.aggregate({}, { groupBy: 'foo.1', total: true }))
					.then((result) => {
						let expected = [ { key: [ 2 ], total: 2 } ];
						expect(result).to.deep.equal(expected);
					});
			});

			it('should support grouping by array index fields 2', function() {
				let model = createModel('Testings', { foo: [ Number ] });
				return model.insertMulti([ { foo: [ 1, 2 ] }, { foo: [] } ])
					.then(() => model.aggregate({}, { groupBy: 'foo.1', total: true }))
					.then((result) => {
						let expected = [
							{ key: [ 2 ], total: 1 },
							{ key: [ null ], total: 1 }
						];
						expect(result).to.deep.equal(expected);
					});
			});


			it('should support sum aggregates', function() {
				let model = createModel('Testings', {
					foo: String,
					bar: Number
				});

				return model.insertMulti([
					{ foo: 'a', bar: 1 },
					{ foo: 'b', bar: 2 },
					{ foo: 'b', bar: 3 },
					{ foo: 'b', bar: 5 }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: 'foo',
							stats: {
								bar: { sum: true }
							}
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 'a' ],
								stats: {
									bar: { sum: 1 }
								}
							}, {
								key: [ 'b' ],
								stats: {
									bar: { sum: 10 }
								}
							}
						]);
					});
			});

			it('should support stddev aggregates', function() {
				let model = createModel('Testings', {
					foo: String,
					bar: Number
				});

				return model.insertMulti([
					{ foo: 'a', bar: 1 },
					{ foo: 'b', bar: 2 },
					{ foo: 'b', bar: 4 }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: 'foo',
							stats: {
								bar: { stddev: true }
							}
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 'a' ],
								stats: {
									bar: { stddev: 0 }
								}
							}, {
								key: [ 'b' ],
								stats: {
									bar: { stddev: 1 }
								}
							}
						]);
					});
			});

			it('should run aggregates with ranges', function() {
				let model = createModel('Testings', {
					foo: Number,
					bar: String,
					baz: Date
				});

				return model.insertMulti([
					{ foo: 0, baz: new Date('2000-04-04T10:10:10Z') },
					{ foo: 0, baz: new Date('2000-04-04T10:10:20Z') },
					{ foo: 1, baz: new Date('2000-02-01T00:00:00Z') },
					{ foo: 2, baz: new Date('2000-10-01T00:00:00Z') },
					{ foo: 3, baz: new Date('2001-02-01T00:00:00Z') },
					{ foo: 4, baz: new Date('2001-08-01T00:00:00Z') },
					{ foo: 5, baz: new Date('2001-11-05T00:00:00Z') },
					{ foo: 5, baz: new Date('2001-11-25T00:00:00Z') },
					{ foo: 5, baz: new Date('2007-02-25T00:00:00Z') }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ {
								field: 'foo',
								ranges: [
									{ end: 1 },
									{ start: 1, end: 3 },
									{ start: 3, end: 4 },
									{ start: 4 }
								]
							} ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 0 ],
								total: 2
							}, {
								key: [ 1 ],
								total: 2
							}, {
								key: [ 2 ],
								total: 1
							}, {
								key: [ 3 ],
								total: 4
							}
						]);
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ {
								field: 'baz',
								ranges: [
									{ end: '2000-03-01T00:00:00Z' },
									{ start: '2000-03-01T00:00:00Z', end: '2001-10-01T00:00:00Z' },
									{ start: '2001-10-01T00:00:00Z' }
								]
							} ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 0 ],
								total: 1
							}, {
								key: [ 1 ],
								total: 5
							}, {
								key: [ 2 ],
								total: 3
							}
						]);
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ {
								field: 'foo',
								ranges: [ 1, 3, 4 ]
							} ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 0 ],
								total: 2
							}, {
								key: [ 1 ],
								total: 2
							}, {
								key: [ 2 ],
								total: 1
							}, {
								key: [ 3 ],
								total: 4
							}
						]);
					});
			});

			it('should run aggregates with intervals', function() {
				let model = createModel('Testings', {
					foo: Number,
					bar: String,
					baz: Date
				});

				let error;
				return model.insertMulti([
					{ foo: -6, bar: 'b', baz: new Date('2005') },
					{ foo: -5, bar: 'b', baz: new Date('2005') },
					{ foo: -4, bar: 'b', baz: new Date('2004') },
					{ foo: -3, bar: 'a', baz: new Date('2003') },
					{ foo: -2, bar: 'b', baz: new Date('2002') },
					{ foo: -1, bar: 'b', baz: new Date('2001') },
					{ foo: 0, bar: 'a', baz: new Date('2000') },
					{ foo: 1, bar: 'b', baz: new Date('2001') },
					{ foo: 2, bar: 'b', baz: new Date('2002') },
					{ foo: 3, bar: 'a', baz: new Date('2003') },
					{ foo: 4, bar: 'b', baz: new Date('2004') },
					{ foo: 5, bar: 'b', baz: new Date('2005') },
					{ foo: 5, baz: new Date('2005', '06') },
					{ foo: 6, bar: 'b', baz: new Date('2005') }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'foo', interval: 2 } ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{ key: [ -6 ], total: 2 },
							{ key: [ -4 ], total: 2 },
							{ key: [ -2 ], total: 2 },
							{ key: [ 0 ], total: 2 },
							{ key: [ 2 ], total: 2 },
							{ key: [ 4 ], total: 3 },
							{ key: [ 6 ], total: 1 }
						].sort(keySort));
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'foo', interval: 3, base: 2 } ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{ key: [ -7 ], total: 2 },
							{ key: [ -4 ], total: 3 },
							{ key: [ -1 ], total: 3 },
							{ key: [ 2 ], total: 3 },
							{ key: [ 5 ], total: 3 }
						].sort(keySort));
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'baz', interval: 'P1Y' } ],
							total: true
						})
							.catch((err) => {
								error = err;
							});
					})
					.then(() => {
						expect(error).to.be.an.instanceof(XError);
						expect(error.code).to.equal(XError.UNSUPPORTED_OPERATION);
					});
			});

			it('should run aggregates with time components', function() {
				let model = createModel('Testings', {
					foo: Number,
					bar: String,
					baz: Date
				});

				return model.insertMulti([
					{ foo: 0, baz: new Date('2000-04-04T10:10:10Z') },
					{ foo: 0, baz: new Date('2000-04-04T10:10:20Z') },
					{ foo: 1, baz: new Date('2000-02') },
					{ foo: 2, baz: new Date('2000-10') },
					{ foo: 3, baz: new Date('2001-02') },
					{ foo: 4, baz: new Date('2001-08') },
					{ foo: 5, baz: new Date('2001-11-05') },
					{ foo: 5, baz: new Date('2001-11-25') },
					{ foo: 5, baz: new Date('2007-02-25') }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'baz', timeComponent: 'year' } ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ '2000-01-01T00:00:00Z' ],
								total: 4
							}, {
								key: [ '2001-01-01T00:00:00Z' ],
								total: 4
							}, {
								key: [ '2007-01-01T00:00:00Z' ],
								total: 1
							}
						]);
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'baz', timeComponent: 'year', timeComponentCount: 2 } ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ '2000-01-01T00:00:00Z' ],
								total: 8
							}, {
								key: [ '2006-01-01T00:00:00Z' ],
								total: 1
							}
						]);
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'baz', timeComponent: 'month', timeComponentCount: 3 } ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ '2000-01-01T00:00:00Z' ],
								total: 1
							}, {
								key: [ '2000-04-01T00:00:00Z' ],
								total: 2
							}, {
								key: [ '2000-10-01T00:00:00Z' ],
								total: 1
							}, {
								key: [ '2001-01-01T00:00:00Z' ],
								total: 1
							}, {
								key: [ '2001-07-01T00:00:00Z' ],
								total: 1
							}, {
								key: [ '2001-10-01T00:00:00Z' ],
								total: 2
							}, {
								key: [ '2007-01-01T00:00:00Z' ],
								total: 1
							}
						]);
					});
			});

			it('should support aggregates accross map fields', function() {
				let model = createModel('Testings', {
					orderTotal: map({}, {
						count: String
					})
				});

				return model.insertMulti([
					{ orderTotal: {
						'2014': { count: 2 }
					} },
					{ orderTotal: {
						'2014': { count: 2 }
					} },
					{ orderTotal: {
						'2014': { count: 6 }
					} }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: 'orderTotal.2014.count',
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.include.members([
							{
								key: [ '2' ],
								total: 2
							},
							{
								key: [ '6' ],
								total: 1
							}
						]);
					});
			});

			it('should group aggregates by multiple fields', function() {
				let model = createModel('Testings', {
					foo: String,
					bar: String,
					baz: Number
				});

				return model.insertMulti([
					{ foo: 'a', bar: 'x', baz: 1 },
					{ foo: 'a', bar: 'y', baz: 2 },
					{ foo: 'a', bar: 'y', baz: 3 },
					{ foo: 'b', bar: 'x', baz: 4 },
					{ foo: 'b', bar: 'y', baz: 5 },
					{ foo: 'b', bar: 'y', baz: 6 },
					{ foo: 'b', baz: 6 }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'foo' }, 'baz' ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 'a', 1 ],
								total: 1
							}, {
								key: [ 'a', 2 ],
								total: 1
							}, {
								key: [ 'a', 3 ],
								total: 1
							}, {
								key: [ 'b', 4 ],
								total: 1
							}, {
								key: [ 'b', 5 ],
								total: 1
							}, {
								key: [ 'b', 6 ],
								total: 2
							}
						]);
					})
					.then(() => {
						return model.aggregate({}, {
							groupBy: [
								{ field: 'foo' },
								{ field: 'baz', interval: 2, base: 1 }
							],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 'a', 1 ],
								total: 2
							}, {
								key: [ 'a', 3 ],
								total: 1
							}, {
								key: [ 'b', 3 ],
								total: 1
							}, {
								key: [ 'b', 5 ],
								total: 3
							}
						]);
					});
			});

			it('should group by array fields', function() {
				let model = createModel('Testings', {
					foo: [ String ],
					bar: Number
				});

				return model.insertMulti([
					{ foo: [ 'a' ], bar: 1 },
					{ foo: [ 'a', 'b' ], bar: 2 },
					{ foo: [ 'b' ], bar: 3 }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: [ { field: 'foo' } ],
							total: true
						});
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 'a' ],
								total: 2
							}, {
								key: [ 'b' ],
								total: 2
							}
						]);
					});
			});

			it('should support the limit option for aggregates', function() {
				let model = createModel('Testings', {
					foo: String,
					bar: Number
				});

				return model.insertMulti([
					{ foo: 'a', bar: 1 },
					{ foo: 'a', bar: 2 },
					{ foo: 'b', bar: 4 },
					{ foo: 'c', bar: 8 }
				])
					.then(() => {
						return model.aggregate({}, {
							groupBy: 'foo',
							total: true
						}, { limit: 2 });
					})
					.then((result) => {
						// Note: Can't test for exact matching because exact order (and contents)
						// of result is undefined here.
						expect(result.length).to.equal(2);
					});
			});

			it('should support the scanLimit option for aggregates', function() {
				let model = createModel('Testings', {
					foo: String,
					bar: Number
				});

				return model.insertMulti([
					{ foo: 'a', bar: 1 },
					{ foo: 'a', bar: 2 },
					{ foo: 'a', bar: 4 },
					{ foo: 'b', bar: 8 }
				])
					.then(() => {
						return model.aggregate(
							{
								foo: 'a'
							},
							{
								groupBy: 'foo',
								stats: {
									bar: {
										avg: true,
										count: true
									}
								},
								total: true
							},
							{
								scanLimit: 2
							}
						);
					})
					.then((result) => {
						expect(result).to.deep.equal([
							{
								key: [ 'a' ],
								total: 2,
								stats: {
									bar: {
										count: 2,
										avg: 1.5
									}
								}
							}
						]);
					});
			});

			it('#aggregateMulti returns an aggregate result if the query matches no documents', function() {
				let model = createModel('Testings', {
					foo: Number,
					bar: Boolean,
					baz: String
				});

				let query = { $nor: [ {} ] };
				let aggregateSpec = {
					a: {
						stats: { foo: { avg: true, count: true, max: true } },
						total: true
					},
					b: {
						stats: { bar: { count: true } }
					},
					c: {
						groupBy: [ { field: 'baz' } ]
					}
				};

				return model.aggregateMulti(query, aggregateSpec)
					.then((result) => {
						expect(result).to.deep.equal({
							a: { total: 0 },
							b: {},
							c: []
						});
					});
			});

			it.skip('should support timeout option in MongoModel#aggregate', function() {
				let model = createModel('Testings', { foo: Number, bar: Boolean, baz: Boolean });

				let records = [];
				for (let r = 0; r < 10000; r++) {
					records.push({ foo: r, bar: true, baz: true });
				}

				return model.insertMulti(records)
				.then(() => model.aggregate({}, { stats: 'foo' }, { timeout: 0.001 }))
				.then(
					() => {
						throw new Error('aggregate should have failed');
					},
					(ex) => {
						expect(ex).to.be.an.instanceof(XError);
						expect(ex.code).to.equal(XError.TIMED_OUT);
						expect(ex.cause).to.be.an.instanceof(Error);
					}
				);
			});
		}
	});

	it('should allow indexed arrays inside objects', function() {
		let model = createModel('Testings', {
			credit: {
				creditCards: [ {
					suffix: {
						type: String,
						index: true
					}
				} ]
			}
		});
		return model.insertMulti([
			{
				credit: {
					creditCards: [
						{ suffix: 'asdf' }
					]
				}
			}
		])
			.then(() => {
				return model.find({
					'credit.creditCards.suffix': 'asdf'
				});
			})
			.then(([ result ]) => {
				expect(result.data.credit.creditCards[0].suffix).to.equal('asdf');
			});
	});

	// The following three tests were used to develop the timeout option, but they are sensitive to
	// the speed of the machine running them and are thus not guaranteed to produce useful results.
	// They're included in case they're needed again, but marked to be skipped so that they don't
	// intefere with the normal test run.

	it.skip('should support timeout option in MongoModel#count', function() {
		this.timeout(6000);

		let model = createModel('Testings', { foo: Boolean });

		let records = [];
		for (let i = 0; i < 50000; i++) {
			records.push({ foo: true });
		}

		return model.insertMulti(records)
		.then(() => model.count({ foo: true }, { timeout: 0.001 }))
		.then(
			() => {
				throw new Error('count should have failed');
			},
			(ex) => {
				expect(ex).to.be.an.instanceof(XError);
				expect(ex.code).to.equal(XError.TIMED_OUT);
				expect(ex.cause).to.be.an.instanceof(Error);
			}
		);
	});

	it.skip('should support timeout option in MongoModel#find', function() {
		let model = createModel('Testings', { foo: Number, bar: Boolean, baz: Boolean });

		let records = [];
		for (let r = 0; r < 10000; r++) {
			records.push({ foo: r, bar: true, baz: true });
		}

		return model.insertMulti(records)
		.then(() => model.find({}, { timeout: 0.001 }))
		.then(
			() => {
				throw new Error('find should have failed');
			},
			(ex) => {
				expect(ex).to.be.an.instanceof(XError);
				expect(ex.code).to.equal(XError.TIMED_OUT);
				expect(ex.cause).to.be.an.instanceof(	Error);
			}
		);
	});

	describe('geoNear conversion', function() {

		it('should work with $near queries', function() {
			let model = createModel('Testings', { point: { type: 'geopoint', index: true } });
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ] },
					{ point: [ 84.2, 39.2 ] },
					{ point: [ 20, 88 ] }
				]))
				.then(() => model.find({
					point: {
						$near: {
							$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
							$maxDistance: 100000
						}
					}
				}))
				.then((documents) => {
					expect(documents.length).to.equal(2);
				});
		});

		it('should throw with more than one $near', function() {
			let model = createModel('Testings', { point: { type: 'geopoint', index: true } });
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ] },
					{ point: [ 84.2, 39.2 ] },
					{ point: [ 20, 88 ] }
				]))
				.then(() => model.find({
					$and: [
						{
							point: {
								$near: {
									$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] }
								}
							}
						}, {
							point: {
								$near: {
									$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] }
								}
							}
						}
					]
				}))
				.then(() => {
					throw new XError(XError.INTERNAL_ERROR, 'Expected rejection');
				}, (err) => {
					expect(err.message).to.match(/can only be used once/);
				});
		});

		it('should throw without an index', function() {
			let model = createModel('Testings', { point: { type: 'geopoint' } });
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ] },
					{ point: [ 84.2, 39.2 ] },
					{ point: [ 20, 88 ] }
				]))
				.then(() => model.find({
					point: {
						$near: {
							$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
							$maxDistance: 100000
						}
					}
				}))
				.then(() => {
					throw new XError(XError.INTERNAL_ERROR, 'Expected rejection');
				}, (err) => {
					expect(err.message).to.match(/No suitable index/);
				});
		});

		it('should throw if $near is inside $or', function() {
			let model = createModel('Testings', { point: { type: 'geopoint', index: true } });
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ] },
					{ point: [ 84.2, 39.2 ] },
					{ point: [ 20, 88 ] }
				]))
				.then(() => model.find({
					$or: [
						{
							point: {
								$near: {
									$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
									$maxDistance: 100000
								}
							}
						},
						{
							point: { $exists: false }
						}
					]
				}))
				.then(() => {
					throw new XError(XError.INTERNAL_ERROR, 'Expected rejection');
				}, (err) => {
					expect(err.message).to.match(/can only be used/);
				});
		});

		it('should not throw if $near is inside $and', function() {
			let model = createModel('Testings', { point: { type: 'geopoint', index: true } });
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ] },
					{ point: [ 84.2, 39.2 ] },
					{ point: [ 20, 88 ] }
				]))
				.then(() => model.find({
					$and: [
						{
							point: {
								$near: {
									$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
									$maxDistance: 100000
								}
							}
						},
						{
							point: { $exists: false }
						}
					]
				}));
		});

		it('should work with fields', function() {
			let model = createModel('Testings', { point: { type: 'geopoint', index: true }, foo: Number });
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ], foo: 1 },
					{ point: [ 84.2, 39.2 ], foo: 2 },
					{ point: [ 20, 88 ], foo: 3 }
				]))
				.then(() => model.find({
					point: {
						$near: {
							$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
							$maxDistance: 100000
						}
					}
				}, {
					fields: [ 'foo' ]
				}))
				.then((documents) => {
					expect(documents[0].data.point).to.not.exist;
					expect(documents[0].data.foo).to.exist;
				});
		});

		it('should work with sort', function() {
			let model = createModel('Testings', { point: { type: 'geopoint', index: true }, foo: Number });
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ], foo: 1 },
					{ point: [ 84.2, 39.2 ], foo: 2 },
					{ point: [ 84.3, 39.3 ], foo: 3 }
				]))
				.then(() => model.find({
					point: {
						$near: {
							$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
							$maxDistance: 100000
						}
					}
				}, {
					sort: [ '-foo' ]
				}))
				.then((documents) => {
					expect(documents[0].data.foo).to.equal(3);
				});
		});

	});


	describe('geoHashed indexes', function() {

		let model;
		beforeEach(function() {
			let stepConfig = { base: 0.01, multiplier: 5, stepNum: 4 };
			model = createModel('Testings', {
				brandId: { type: 'string' },
				point: {
					type: 'geopoint',
					index: {
						indexType: 'geoHashed',
						step: stepConfig
					}
				},
				multiPoint: {
					type: 'geopoint',
					index: {
						indexType: 'geoHashed',
						step: stepConfig
					}
				}
			});
			model.index({ brandId: 1, point: { indexType: 'geoHashed', step: stepConfig } });
		});

		it('should work with $near queries', function() {
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ] },
					{ point: [ 84.2, 39.2 ] },
					{ point: [ 20, 88 ] }
				]))
				.then(() => model.find({
					point: {
						$near: {
							$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
							$maxDistance: 100000
						}
					}
				}))
				.then((documents) => {
					expect(documents.length).to.equal(2);
				});
		});

		it('should throw if $near is inside $or', function() {
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ] },
					{ point: [ 84.2, 39.2 ] },
					{ point: [ 20, 88 ] }
				]))
				.then(() => model.find({
					$or: [
						{
							point: {
								$near: {
									$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
									$maxDistance: 100000
								}
							}
						},
						{
							point: { $exists: false }
						}
					]
				}))
				.then(() => {
					throw new XError(XError.INTERNAL_ERROR, 'Expected rejection');
				}, (err) => {
					expect(err.message).to.match(/can only be used/);
				});
		});

		it('should work with fields', function() {
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ], brandId: 'billy-bobs-burger-bayou' },
					{ point: [ 84.2, 39.2 ], brandId: 'billy-bobs-burger-bayou' },
					{ point: [ 20, 88 ], brandId: 'billy-bobs-burger-bayou' }
				]))
				.then(() => model.find({
					point: {
						$near: {
							$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
							$maxDistance: 100000
						}
					}
				}, {
					fields: [ 'brandId' ]
				}))
				.then((documents) => {
					expect(documents.length).to.equal(2);
					expect(documents[0].data.point).to.not.exist;
					expect(documents[0].data.brandId).to.exist;
				});
		});

		it('should work with sort', function() {
			return model.collectionPromise
				.then(() => model.insertMulti([
					{ point: [ 84, 39 ], brandId: 'billy-bobs-burger-bayou' },
					{ point: [ 84.2, 39.2 ], brandId: 'billy-bobs-bacon-bayou' },
					{ point: [ 84.3, 39.3 ], brandId: 'billy-bobs-biscuit-bayou' }
				]))
				.then(() => model.find({
					point: {
						$near: {
							$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
							$maxDistance: 100000
						}
					}
				}, {
					sort: [ 'brandId' ]
				}))
				.then((documents) => {
					expect(documents[0].data.brandId).to.equal('billy-bobs-bacon-bayou');
				});
		});

		it('should automatically sort by distance', function() {
			return model.collectionPromise
				.then(() => {
					return model.insertMulti([
						{ point: [ 84, 39 ], brandId: 'billy-bobs-burger-bayou' },
						{ point: [ 84.11, 39.11 ], brandId: 'billy-bobs-bacon-bayou' },
						{ point: [ 84.3, 39.3 ], brandId: 'billy-bobs-biscuit-bayou' }
					]);
				}).then(() => {
					return model.find({
						point: {
							$near: {
								$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
								$maxDistance: 100000
							}
						}
					});
				}).then((documents) => {
					expect(documents[0].data.brandId).to.equal('billy-bobs-bacon-bayou');
				});
		});

		it('should work with update() and upsert option', function() {
			return model.collectionPromise
				.then(() => {
					return model.update(
						{ brandId: 'billy-bobs-burger-bayou' },
						{ $set: { point: [ 84.11, 39.11 ], brandId: 'billy-bobs-burger-bayou' } },
						{ upsert: true }
					);
				}).then(() => {
					return model.find({
						point: {
							$near: {
								$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
								$maxDistance: 100000
							}
						}
					});
				}).then((documents) => {
					expect(documents[0].data.brandId).to.equal('billy-bobs-burger-bayou');
				});
		});

		// For the same reasons as the other timout tests above, this test is
		// marked as skipped
		it.skip('should work with timeout', function() {
			let records = [];
			for (let r = 0; r < 10000; r++) {
				records.push({
					point: [ 84.1, 39.1 ],
					brandId: `brand-${r}`
				});
			}

			return model.collectionPromise
				.then(() => model.insertMulti(records))
				.then(() => model.find(
					{
						point: {
							$near: {
								$geometry: { type: 'Point', coordinates: [ 84.1, 39.1 ] },
								$maxDistance: 100000
							}
						}
					},
					{ timeout: 0.001 }
				))
				.then(
					() => {
						throw new Error('find should have failed');
					},
					(ex) => {
						expect(ex).to.be.an.instanceof(XError);
						expect(ex.code).to.equal(XError.TIMED_OUT);
						expect(ex.cause).to.be.an.instanceof(Error);
					}
				);
		});

	});
});
