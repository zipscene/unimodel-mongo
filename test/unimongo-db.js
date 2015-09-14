const expect = require('chai').expect;
const UnimongoDb = require('../lib').UnimongoDb;
const UnimongoError = require('../lib').UnimongoError;
const pasync = require('pasync');
const testScaffold = require('./lib/mongo-scaffold');

describe('UnimongoDb', function() {
	it('should connect to mongo', function(done) {
		let testdb = new UnimongoDb();
		testdb.connect(testScaffold.config.uri)
			.then(() => done())
			.catch(done)
			.catch(pasync.abort);
	});

	it('should emit a connect event', function(done) {
		let testdb = new UnimongoDb();
		testdb.on('connect', () => done());
		testdb.connect(testScaffold.config.uri);
	});

	it('should emit an error event on error', function(done) {
		let testdb = new UnimongoDb();
		testdb.on('connect', () => done(new Error('Unexpected success')));
		testdb.on('error', (err) => {
			expect(err).to.be.an.instanceof(UnimongoError);
			done();
		});
		testdb.connect(testScaffold.config.nonexistantUri);
	});
});
