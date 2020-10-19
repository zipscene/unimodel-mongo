// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const { MongoDb, MongoError } = require('../lib');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const { expect } = chai;
const pasync = require('pasync');
const testScaffold = require('./lib/mongo-scaffold');
const opUtils = require('../lib/utils/ops');
chai.use(sinonChai);

describe('MongoDb', function() {
	let sandbox;
	let testdb;

	beforeEach(function() {
		sandbox = sinon.createSandbox();
		testdb = undefined;
	});

	afterEach(function() {
		sandbox.restore();
		if (testdb) testdb.close();
		testdb = undefined;
	});

	it('should connect to mongo', function(done) {
		testdb = new MongoDb();
		testdb.connect(testScaffold.config.uri)
			.then(() => done())
			.catch(done)
			.catch(pasync.abort);
	});

	it('should emit a connect event', function(done) {
		testdb = new MongoDb();
		testdb.on('connect', () => done());
		testdb.connect(testScaffold.config.uri);
	});

	it.skip('should emit an error event on error', function(done) {
		testdb = new MongoDb();
		testdb.on('connect', () => done(new Error('Unexpected success')));
		testdb.on('error', (err) => {
			expect(err).to.be.an.instanceof(MongoError);
			done();
		});
		testdb.connect(testScaffold.config.nonexistantUri);
	});

	describe('#killOperation', function() {
		it('gets ids of ops with matching commments and kills them', function() {
			testdb = new MongoDb();
			let operationId = 'some-operation-id';
			let opIds = [ 12345, 67890 ];
			let opsKilled = false;
			sinon.stub(testdb, '_getOpIdsWithComment').resolves(opIds);
			sinon.stub(testdb, '_killOps').callsFake(function() {
				return new Promise((resolve) => {
					setImmediate(() => {
						opsKilled = true;
						resolve();
					});
				});
			});

			return testdb.killOperation(operationId)
				.then(() => {
					expect(testdb._getOpIdsWithComment).to.be.calledOnce;
					expect(testdb._getOpIdsWithComment).to.be.calledOn(testdb);
					expect(testdb._getOpIdsWithComment).to.be.calledWith(operationId);
					expect(testdb._killOps).to.be.calledOnce;
					expect(testdb._killOps).to.be.calledOn(testdb);
					expect(testdb._killOps).to.be.calledWith(opIds);
					expect(opsKilled).to.be.true;
				});
		});
	});

	describe('#_getOpIdsWithComment', function() {
		it('resolves with result of currentOp command on admin db', function() {
			testdb = new MongoDb();
			return testdb.connect(testScaffold.config.uri)
				.then(() => {
					let comment = 'some comment';
					let admindb = testdb.db.admin();
					let currentOpDoc = { foo: 'bar' };
					let opIds = [ 'foo', 'bar' ];
					sinon.stub(testdb.db, 'admin').returns(admindb);
					sinon.stub(admindb, 'command').resolves(currentOpDoc);
					sandbox.stub(opUtils, 'getOpIdsWithComment').returns(opIds);

					return testdb._getOpIdsWithComment(comment)
						.then((result) => {
							expect(testdb.db.admin).to.be.calledOnce;
							expect(testdb.db.admin).to.be.calledOn(testdb.db);
							expect(admindb.command).to.be.calledOnce;
							expect(admindb.command).to.be.calledOn(admindb);
							expect(admindb.command).to.be.calledWith({
								currentOp: 1
							});
							expect(opUtils.getOpIdsWithComment).to.be.calledOnce;
							expect(opUtils.getOpIdsWithComment).to.be.calledOn(opUtils);
							expect(opUtils.getOpIdsWithComment).to.be.calledWith(
								currentOpDoc,
								comment
							);
							expect(result).to.equal(opIds);
						});
				});
		});
	});

	describe('#_killOps', function() {
		let testdb, opIds, eachComplete, admindb;

		beforeEach(function() {
			testdb = new MongoDb();
			opIds = [ 12345, 67890 ];
			eachComplete = false;

			sandbox.stub(pasync, 'each').callsFake(function() {
				return new Promise((resolve) => {
					setImmediate(() => {
						eachComplete = true;
						resolve();
					});
				});
			});

			return testdb.connect(testScaffold.config.uri)
				.then(() => {
					admindb = testdb.db.admin();
					sinon.stub(testdb.db, 'admin').returns(admindb);
				});
		});

		afterEach(function() {
			testdb.close();
		});

		it('resolves after pasyn::each on provided op ids', function() {
			return testdb._killOps(opIds)
				.then(() => {
					expect(pasync.each).to.be.calledOnce;
					expect(pasync.each).to.be.calledOn(pasync);
					expect(pasync.each).to.be.calledWith(opIds, sinon.match.func);
					expect(eachComplete).to.be.true;
				});
		});

		describe('pasync::each iteratee', function() {
			let iteratee;

			beforeEach(function() {
				return testdb._killOps(opIds)
					.then(() => {
						iteratee = pasync.each.firstCall.args[1];
					});
			});

			it('kills op with provided opId using admin db', function() {
				let opId = 12345;
				let killOpResult = { foo: 'bar' };
				sinon.stub(admindb, 'command').resolves(killOpResult);

				return iteratee(opId)
					.then((result) => {
						expect(testdb.db.admin).to.be.calledOnce;
						expect(testdb.db.admin).to.be.calledOn(testdb.db);
						expect(admindb.command).to.be.calledOnce;
						expect(admindb.command).to.be.calledOn(admindb);
						expect(admindb.command).to.be.calledWith({
							killOp: 1,
							op: opId
						});
						expect(result).to.equal(killOpResult);
					});
			});
		});
	});
});
