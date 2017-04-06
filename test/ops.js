const ops = require('../lib/utils/ops');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const { expect } = chai;
const objtools = require('objtools');
chai.use(sinonChai);

describe('utils/ops', function() {
	let sandbox;

	beforeEach(function() {
		sandbox = sinon.sandbox.create();
	});

	afterEach(function() {
		sandbox.restore();
	});

	describe('::getQuery', function() {
		it('returns query from provided op', function() {
			let op = { query: {
				find: 'foo',
				filter: { $comment: 'some comment' }
			} };

			expect(ops.getQuery(op)).to.equal(op.query);
		});

		it('returns originatingCommand for getMore ops', function() {
			let op = {
				query: {
					getMore: 12345,
					collection: 'foo',
					batchSize: 1000
				},
				originatingCommand: {
					find: 'foo',
					filter: { $comment: 'some comment' }
				}
			};

			expect(ops.getQuery(op)).to.equal(op.originatingCommand);
		});
	});

	describe('::getCommentFromQuery', function() {
		const comment = 'some comment';

		function expectGetPath(query, path) {
			let result = ops.getCommentFromQuery(query);

			expect(objtools.getPath).to.be.calledOnce;
			expect(objtools.getPath).to.be.calledOn(objtools);
			expect(objtools.getPath).to.be.calledWith(query, path);
			expect(result).to.equal(comment);
		}

		beforeEach(function() {
			sandbox.stub(objtools, 'getPath').returns(comment);
		});

		it('supports find queries', function() {
			expectGetPath({ find: 'foo' }, 'filter.$comment');
		});

		it('supports count queries', function() {
			expectGetPath({ count: 'foo' }, 'query.$comment');
		});

		it('supports aggregate queries', function() {
			expectGetPath({ aggregate: 'foo' }, 'pipeline.0.$match.$comment');
		});

		it('supports truncated queries', function() {
			expectGetPath({ $truncated: 'foo..' }, 'comment');
		});

		it('returns null for all other queries', function() {
			expect(ops.getCommentFromQuery({ other: 'foo' })).to.be.null;
		});

		it('returns null if nothing is found at path', function() {
			objtools.getPath.returns(undefined);

			expect(ops.getCommentFromQuery({ find: 'foo' })).to.be.null;
		});
	});

	describe('::queryHasComment', function() {
		context('object query', function() {
			const comment = 'some comment';
			let query;

			beforeEach(function() {
				sandbox.stub(ops, 'getCommentFromQuery').returns(comment);
			});

			it('returns true if comment from query equals provided commment', function() {
				let result = ops.queryHasComment(query, comment);

				expect(ops.getCommentFromQuery).to.be.calledOnce;
				expect(ops.getCommentFromQuery).to.be.calledOn(ops);
				expect(ops.getCommentFromQuery).to.be.calledWith(query);
				expect(result).to.be.true;
			});

			it('returns false otherwise', function() {
				expect(ops.queryHasComment(query, 'other commment')).to.be.false;
			});
		});

		context('string query', function() {
			it('returns true if comment property occurs in string', function() {
				let comment = 'some comment';
				let otherComment = 'other comment';
				let query = `{ find: "foo", filter: { $comment: "${comment}", "${otherComment}"...`;

				expect(ops.queryHasComment(query, comment)).to.be.true;
				expect(ops.queryHasComment(query, otherComment)).to.be.false;
			});

			it('tolerates whitespace differences', function() {
				let comment = 'some comment';
				let query = `$comment:"${comment}"`;
				let otherQuery = `$comment:  "${comment}"`;

				expect(ops.queryHasComment(query, comment)).to.be.true;
				expect(ops.queryHasComment(otherQuery, comment)).to.be.true;
			});

			it('tolerates both escaped and unescaped double quotes in comment', function() {
				let comment = '"some"comment"';
				let query = `$comment: "${comment}"`;
				let otherQuery = `$comment: "\\"some\\"comment\\""`;
				let anotherQuery = `$comment: \\"${comment}\\"`;

				expect(ops.queryHasComment(query, comment)).to.be.true;
				expect(ops.queryHasComment(otherQuery, comment)).to.be.true;
				expect(ops.queryHasComment(anotherQuery, comment)).to.be.false;
			});

			it('tolerates regexp special characters in comment', function() {
				let comment = '.*';
				let query = `$comment: "${comment}"`;
				let otherQuery = `$comment: "otherComment"`;

				expect(ops.queryHasComment(query, comment)).to.be.true;
				expect(ops.queryHasComment(otherQuery, comment)).to.be.false;
			});
		});
	});


	describe('::hasComment', function() {
		it('gets query from op and checks it for the provided comment', function() {
			let op = { op: 'find' };
			let comment = 'some comment';
			let query = { find: 'foo' };
			sandbox.stub(ops, 'getQuery').returns(query);
			sandbox.stub(ops, 'queryHasComment').returns('has comment result');

			let result = ops.hasComment(op, comment);

			expect(ops.getQuery).to.be.calledOnce;
			expect(ops.getQuery).to.be.calledOn(ops);
			expect(ops.getQuery).to.be.calledWith(op);
			expect(ops.queryHasComment).to.be.calledOnce;
			expect(ops.queryHasComment).to.be.calledOn(ops);
			expect(ops.queryHasComment).to.be.calledWith(query, comment);
			expect(result).to.equal(ops.queryHasComment.firstCall.returnValue);
		});
	});

	describe('::getOpIdsWithComment', function() {
		it('returns opIds of ops with matching comment', function() {
			let fooOp = { op: 'foo', opid: 12345 };
			let barOp = { op: 'bar', opid: 67890 };
			let bazOp = { op: 'baz', opid: 42 };
			let currentOpDoc = { inprog: [ fooOp, barOp, bazOp ] };
			let comment = 'some comment';
			sandbox.stub(ops, 'hasComment')
				.withArgs(fooOp, comment).returns(true)
				.withArgs(barOp, comment).returns(false)
				.withArgs(bazOp, comment).returns(true);

			let result = ops.getOpIdsWithComment(currentOpDoc, comment);

			expect(ops.hasComment).to.be.calledThrice;
			expect(ops.hasComment).to.always.be.calledOn(ops);
			expect(ops.hasComment).to.be.calledWith(fooOp, comment);
			expect(ops.hasComment).to.be.calledWith(barOp, comment);
			expect(ops.hasComment).to.be.calledWith(bazOp, comment);
			expect(result).to.deep.equal([ fooOp.opid, bazOp.opid ]);
		});
	});
});
