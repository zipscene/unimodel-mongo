const chai = require('chai');
const expect = chai.expect;
const { createModel, MongoError } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');
const { map } = require('zs-common-schema');
const bson = require('bson');
const BSON = new bson.BSONPure.BSON();
const objtools = require('zs-objtools');

describe('MongoModel (Map Support)', function() {
	beforeEach(testScaffold.resetAndConnect);

	describe('index', function() {

		it('should convert map indexes into hidden indexed arrays', function() {
			let model = createModel('Testings', {
				aggrs: map({}, {
					total: {
						type: Number,
						index: true
					},
					orderTotal: map({}, {
						count: {
							type: Number,
							index: true
						},
						total: {
							type: Number,
							index: true
						}
					})
				})
			});
			model.index({ 'aggrs.orderTotal.total': 1, 'aggrs.orderTotal.count': 1 });
			return model.collectionPromise
				.then(() => {
					expect(model._indices).to.deep.include.members([
						{ spec: { '_mapidx_aggrs_total': 1 }, options: {} },
						{ spec: { '_mapidx_aggrs|orderTotal_count': 1 }, options: {} },
						{ spec: { '_mapidx_aggrs|orderTotal_total': 1 }, options: {} },
						{ spec: { '_mapidx_aggrs|orderTotal_count_total': 1 }, options: {} }
					]);
				});
		});

		it('should throw when trying to index across multiple maps', function() {
			let Model = createModel('Testings', {
				aggrs: map({}, {
					total: {
						type: Number,
						index: true
					},
					orderTotal: map({}, {
						total: {
							type: Number,
							index: true
						}
					})
				})
			});
			expect(() => Model.index({ 'aggrs.total': 1, 'aggrs.orderTotal.total': 1 }))
				.to.throw(MongoError, 'Cannot index accross maps multiple maps.');
			return Model.collectionPromise;
		});

	});

	describe('query', function() {

		it('should normalize indexed map queries', function() {
			let Model = createModel('Testings', {
				aggrs: map({}, {
					orderTotal: map({}, {
						total: { type: Number, index: true }
					})
				})
			});
			let query = Model.normalizeQuery({
				'aggrs.zs.orderTotal.2014.total': 4,
				$and: [
					{ 'aggrs.zs.orderTotal.2014.total': {
						$gt: 2
					} },
					{ 'aggrs.zs.orderTotal.2014.total': {
						$lte: 2
					} },
					{ 'aggrs.zs.orderTotal.2014.total': {
						$gt: 2,
						$lte: 10
					} }
				]
			});
			let ltbson = BSON.serialize([ 'zs', '2014' ]);
			ltbson[ltbson.length - 1] += 1;
			expect(query.getData()['_mapidx_aggrs|orderTotal_total'])
				.to.equal(BSON.serialize([ 'zs', '2014', 4 ]).toString());
			expect(query.getData().$and).to.deep.include.members([
				{ '_mapidx_aggrs|orderTotal_total': {
					$gt: BSON.serialize([ 'zs', '2014', 2 ]).toString(),
					$lt: ltbson.toString()
				} },
				{ '_mapidx_aggrs|orderTotal_total': {
					$lte: BSON.serialize([ 'zs', '2014', 2 ]).toString(),
					$gte: BSON.serialize([ 'zs', '2014' ]).toString()
				} },
				{ '_mapidx_aggrs|orderTotal_total': {
					$gt: BSON.serialize([ 'zs', '2014', 2 ]).toString(),
					$lte: BSON.serialize([ 'zs', '2014', 10 ]).toString()
				} }
			]);
			return Model.collectionPromise;
		});

		it('should normalize compound indexed map queries', function() {
			let Model = createModel('Testings', {
				aggrs: map({}, {
					orderTotal: map({}, {
						count: Number,
						total: Number
					})
				})
			});
			Model.index({ 'aggrs.orderTotal.total': 1, 'aggrs.orderTotal.count': 1 });
			let query = Model.normalizeQuery({
				'aggrs.zs.orderTotal.2014.total': 4,
				'aggrs.zs.orderTotal.2014.count': 2
			});
			expect(query.getData()).to.deep.equal({
				'_mapidx_aggrs|orderTotal_count_total': BSON.serialize([ 'zs', '2014', 2, 4 ]).toString()
			});
			return Model.collectionPromise;
		});

		it.only('should find using indexed maps', function() {
			let Model = createModel('Testings', {
				orderTotal: map({}, {
					count: { type: Number, index: true }
				})
			});
			return Model.insertMulti([
				{ orderTotal: { 2014: { count: 2 } } },
				{ orderTotal: { 2014: { count: 5 } } },
				{ orderTotal: { 2014: { count: 1 } } },
				{ orderTotal: { 2015: { count: 10 } } }
			])
				.then(() => Model.find({
					'orderTotal.2014.count': { $gte: 2, $lt: 9999999 }
				}))
				.then((docs) => {
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(2);
				});
		});

		it('should find using compound indexed maps', function() {
			let Model = createModel('Testings', {
				orderTotal: map({}, {
					count: Number,
					total: Number
				})
			});
			Model.index({ 'orderTotal.total': 1, 'orderTotal.count': 1 });
			return Model.insertMulti([
				{ orderTotal: { 2014: { count: 2, total: 1 } } },
				{ orderTotal: { 2014: { count: 5, total: 1 } } },
				{ orderTotal: { 2014: { count: 2, total: 1 } } }
			])
				.then(() => Model.find({
					'orderTotal.2014.count': 2,
					'orderTotal.2014.total': 1
				}))
				.then((docs) => {
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(2);
				});
		});

	});

	describe('schema', function() {

		it('should convert indexed maps into hidden fields in documents', function() {
			let Model = createModel('Testings', {
				aggrs: map({}, {
					total: {
						type: Number,
						index: true
					},
					orderTotal: map({}, {
						count: {
							type: Number,
							index: true
						},
						total: {
							type: Number,
							index: true
						}
					})
				})
			});
			Model.index({ 'aggrs.orderTotal.total': 1, 'aggrs.orderTotal.count': 1 });
			return Model.collectionPromise
				.then(() => {
					let doc = Model.create({
						aggrs: {
							zs: {
								total: 3,
								orderTotal: {
									'2014-09': {
										count: 5,
										total: 2
									},
									'2014-10': {
										count: 10,
										total: 1
									}
								}
							},
							marcos: {
								total: 123,
								orderTotal: {
									'2015-01': {
										count: 3,
										total: 123
									}
								}
							}
						}
					});
					return doc.save()
						.then(() => {
							let docData = objtools.deepCopy(doc.getData());
							Model.normalizeDocumentIndexedMapValues(docData);
							expect(docData['_mapidx_aggrs|orderTotal_count']).to.deep.include.members([
								BSON.serialize([ 'zs', '2014-09', 5 ]).toString(),
								BSON.serialize([ 'zs', '2014-10', 10 ]).toString(),
								BSON.serialize([ 'marcos', '2015-01', 3 ]).toString()
							]);
							expect(docData['_mapidx_aggrs|orderTotal_total']).to.deep.include.members([
								BSON.serialize([ 'zs', '2014-09', 2 ]).toString(),
								BSON.serialize([ 'zs', '2014-10', 1 ]).toString(),
								BSON.serialize([ 'marcos', '2015-01', 123 ]).toString()
							]);
							expect(docData['_mapidx_aggrs_total']) //eslint-disable-line dot-notation
								.to.deep.include.members([
									BSON.serialize([ 'zs', 3 ]).toString(),
									BSON.serialize([ 'marcos', 123 ]).toString()
								]);
							expect(docData['_mapidx_aggrs|orderTotal_count_total']).to.deep.include.members([
								BSON.serialize([ 'zs', '2014-09', 5, 2 ]).toString(),
								BSON.serialize([ 'zs', '2014-10', 10, 1 ]).toString(),
								BSON.serialize([ 'marcos', '2015-01', 3, 123 ]).toString()
							]);
						});
				});
		});

	});

});
