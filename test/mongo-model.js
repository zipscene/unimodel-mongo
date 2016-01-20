const chai = require('chai');
const XError = require('xerror');
const expect = chai.expect;
const { MongoDocument, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');
const { map } = require('zs-common-schema');

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

	it('should recognize the appropriate indices', function() {
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

	it('should create indices', function() {
		let model = createModel('Testings', {
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
			return createModel('Testings', {
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

	it('MongoModel#upsert should create a document if none match the query', function() {
		let model = createModel('Testings', { foo: String });

		return model.upsert({ foo: 'bar' }, { foo: 'baz' })
			.then((numUpdated) => {
				expect(numUpdated).to.equal(0);
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
			.then((numUpdated) => {
				expect(numUpdated).to.equal(1);
			})
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
				sort: [ 'bar', '-foo' ]
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
				sort: [ 'bar', '-foo' ]
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
				let fn = () => {
					return model.aggregate({}, {
						groupBy: [ { field: 'baz', interval: 'P1Y' } ],
						total: true
					});
				};

				expect(fn).to.throw(XError);
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

});
