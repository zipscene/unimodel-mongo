const chai = require('chai');
const expect = chai.expect;
const { UnimongoDocument, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');

chai.use(require('chai-as-promised'));

const keySort = (a, b) => {
	let aString = ''+a.key;
	let bString = ''+b.key;
	if (aString > bString) return 1;
	if (aString < bString) return -1;
	return 0;
};

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

	it('should flag existing documents with `isExisting`', function() {
		let model = createModel('testings', { foo: Number });

		return model.insert({ foo: '123' })
			.then((document) => {
				expect(document.options.isExisting).to.be.true;
				expect(document.getInternalId()).to.exist;
			});
	});

	it('should insert documents into the collection', function() {
		let model = createModel('testings', { foo: Number });

		return model.insert({ foo: '123' })
			.then((document) => {
				expect(document.data.foo).to.equal(123);
				expect(document.getInternalId()).to.exist;
			});
	});

	it('should insert multiple documents into the collection', function() {
		let model = createModel('testings', { foo: Number });

		return model.insertMulti([ { foo: '123' }, { foo: '234' } ])
			.then((results) => {
				expect(Array.isArray(results)).to.be.true;
				expect(results[0].data.foo).to.equal(123);
				expect(results[0].getInternalId()).to.exist;
				expect(results[1].data.foo).to.equal(234);
				expect(results[1].getInternalId()).to.exist;
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

	it('should run aggregates', function() {
		let model = createModel('testings', {
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
				return model.aggregate({
					foo: 'a'
				}, {
					stats: {
						baz: { count: true, avg: true, min: true, max: true }
					},
					total: true
				});
			})
			.then((result) => {
				expect(result).to.deep.equal({
					stats: {
						baz: { count: 3, avg: 2, min: 1, max: 3 }
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

	it('should run interval aggregates', function() {
		let model = createModel('testings', {
			foo: Number,
			bar: String,
			baz: Date
		});

		return model.insertMulti([
			{ foo: 0, bar: 'a', baz: new Date('2000') },
			{ foo: 1, bar: 'b', baz: new Date('2001') },
			{ foo: 2, bar: 'b', baz: new Date('2002') },
			{ foo: 3, bar: 'a', baz: new Date('2003') },
			{ foo: 4, bar: 'b', baz: new Date('2004') },
			{ foo: 5, bar: 'b', baz: new Date('2005') },
			{ foo: 5, baz: new Date('2005', '06') }
		])
			.then(() => {
				return model.aggregate({}, {
					groupBy: [ { field: 'foo', interval: 2 } ],
					total: true
				});
			})
			.then((result) => {
				expect(result).to.deep.equal([
					{
						key: [ 0 ],
						total: 2
					}, {
						key: [ 2 ],
						total: 2
					}, {
						key: [ 4 ],
						total: 3
					}
				]);
			})
			.then(() => {
				return model.aggregate({}, {
					groupBy: [ { field: 'foo', interval: 2, base: 1 } ],
					total: true
				});
			})
			.then((result) => {
				expect(result).to.deep.equal([
					{
						key: [ 1 ],
						total: 2
					}, {
						key: [ 3 ],
						total: 2
					}, {
						key: [ 5 ],
						total: 3
					}
				]);
			})
			.then(() => {
				return model.aggregate({}, {
					groupBy: [ { field: 'baz' } ],
					// groupBy: [ { field: 'baz', interval: 'P1Y' } ],
					total: true
				});
			})
			.then((result) => {
				console.log(result);
			});
	});

	it('should group aggregates by multiple fields', function() {
		let model = createModel('testings', {
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
			});
	});
});
