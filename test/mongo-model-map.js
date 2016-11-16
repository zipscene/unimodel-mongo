// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const { expect } = require('chai');
const { createModel, MongoModel, MongoError } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');
const { map } = require('common-schema');
const bson = require('bson');
const BSON = new bson.BSONPure.BSON();
const objtools = require('objtools');

const hash = MongoModel.createMapIndexHash;

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
					expect(model._indexes).to.deep.include.members([
						{ spec: { [hash('aggrs^total')]: 1 }, options: {} },
						{ spec: { [hash('aggrs|orderTotal^count')]: 1 }, options: {} },
						{ spec: { [hash('aggrs|orderTotal^total')]: 1 }, options: {} },
						{ spec: { [hash('aggrs|orderTotal^count^total')]: 1 }, options: {} }
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
			return Model.collectionPromise.then(() => {
				let gtbsonA = BSON.serialize([ 'zs', '2014', 2 ]);
				let ltbsonA = BSON.serialize([ 'zs', '2014' ]);
				ltbsonA[ltbsonA.length - 2] += 1;
				ltbsonA[0] = gtbsonA[0];
				let ltbsonB = BSON.serialize([ 'zs', '2014', 2 ]);
				let gtbsonB = BSON.serialize([ 'zs', '2014' ]);
				gtbsonB[0] = ltbsonB[0];
				expect(query.getData()[hash('aggrs|orderTotal^total')])
					.to.equal(BSON.serialize([ 'zs', '2014', 4 ]).toString());
				expect(query.getData().$and).to.deep.include.members([
					{ [hash('aggrs|orderTotal^total')]: {
						$gt: gtbsonA.toString(),
						$lt: ltbsonA.toString()
					} },
					{ [hash('aggrs|orderTotal^total')]: {
						$lte: ltbsonB.toString(),
						$gte: gtbsonB.toString()
					} },
					{ [hash('aggrs|orderTotal^total')]: {
						$gt: BSON.serialize([ 'zs', '2014', 2 ]).toString(),
						$lte: BSON.serialize([ 'zs', '2014', 10 ]).toString()
					} }
				]);
			});
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
			return Model.collectionPromise.then(() => {
				expect(query.getData()).to.deep.equal({
					[hash('aggrs|orderTotal^count^total')]: BSON.serialize([ 'zs', '2014', 2, 4 ]).toString()
				});
			});
		});

		it('should find using indexed maps', function() {
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
					'orderTotal.2014.count': { $gte: 2 }
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

		it('should normalize inside logical join operators', function() {
			let Model = createModel('Testings', {
				orderTotal: map({}, {
					count: { type: Number, index: true }
				})
			});
			return Model.collectionPromise.then(() => {
				let query = Model.normalizeQuery({
					$and: [
						{ $or: [
							{ $nor: [
								{ 'orderTotal.2014.count': 5 }
							] }
						] }
					]
				});

				expect(query.getData()).to.deep.equal({
					$and: [
						{ $or: [
							{ $nor: [
								{ [hash('orderTotal^count')]: BSON.serialize([ '2014', 5 ]).toString() }
							] }
						] }
					]
				});
			});
		});

		it('should not map normalize if no map fields are present', function() {
			let Model = createModel('Testings', {
				orderTotal: { 2014: {
					count: { type: Number, index: true }
				} }
			});
			return Model.collectionPromise.then(() => {
				let rawQuery = {
					'orderTotal.2014.count': 5
				};
				let query = Model.normalizeQuery(rawQuery);
				expect(query.getData()).to.deep.equal(rawQuery);
			});
		});

		it('should not map normalize if multiple maps are present', function() {
			let Model = createModel('Testings', {
				aggrs: map({}, {
					count: { type: Number, index: true },
					orderTotal: map({}, {
						count: { type: Number, index: true }
					})
				})
			});
			return Model.collectionPromise.then(() => {
				let rawQuery = {
					'aggrs.zs.orderTotal.2014.count': 2,
					'aggrs.zs.count': 5
				};
				let query = Model.normalizeQuery(rawQuery);
				expect(query.getData()).to.deep.equal(rawQuery);
			});

		});

		it('should not map normalize if invalid operators are found', function() {
			let Model = createModel('Testings', {
				orderTotal: map({}, {
					count: { type: Number, index: true }
				})
			});
			return Model.collectionPromise.then(() => {
				let rawQuery = {
					'orderTotal.2014.count': {
						$not: {
							$gt: 5
						}
					}
				};
				let query = Model.normalizeQuery(rawQuery);
				expect(query.getData()).to.deep.equal(rawQuery);
			});
		});

		it('should not map normalize if the field is not indexed', function() {
			let Model = createModel('Testings', {
				orderTotal: map({}, {
					count: Number
				})
			});
			return Model.collectionPromise.then(() => {
				let rawQuery = {
					'orderTotal.2014.count': 5
				};
				let query = Model.normalizeQuery(rawQuery);
				expect(query.getData()).to.deep.equal(rawQuery);
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
							expect(docData[hash('aggrs|orderTotal^count')]).to.deep.include.members([
								BSON.serialize([ 'zs', '2014-09', 5 ]).toString(),
								BSON.serialize([ 'zs', '2014-10', 10 ]).toString(),
								BSON.serialize([ 'marcos', '2015-01', 3 ]).toString()
							]);
							expect(docData[hash('aggrs|orderTotal^total')]).to.deep.include.members([
								BSON.serialize([ 'zs', '2014-09', 2 ]).toString(),
								BSON.serialize([ 'zs', '2014-10', 1 ]).toString(),
								BSON.serialize([ 'marcos', '2015-01', 123 ]).toString()
							]);
							expect(docData[hash('aggrs^total')])
								.to.deep.include.members([
									BSON.serialize([ 'zs', 3 ]).toString(),
									BSON.serialize([ 'marcos', 123 ]).toString()
								]);
							expect(docData[hash('aggrs|orderTotal^count^total')]).to.deep.include.members([
								BSON.serialize([ 'zs', '2014-09', 5, 2 ]).toString(),
								BSON.serialize([ 'zs', '2014-10', 10, 1 ]).toString(),
								BSON.serialize([ 'marcos', '2015-01', 3, 123 ]).toString()
							]);
						});
				});
		});

	});

});
