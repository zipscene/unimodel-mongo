const expect = require('chai').expect;
const MongoDb = require('../lib').MongoDb;
const MongoError = require('../lib').MongoError;
const pasync = require('pasync');
const testScaffold = require('./lib/mongo-scaffold');

describe('MongoDb', function() {
	it('should connect to mongo', function(done) {
		let testdb = new MongoDb();
		testdb.connect(testScaffold.config.uri)
			.then(() => done())
			.catch(done)
			.catch(pasync.abort);
	});

	it('should emit a connect event', function(done) {
		let testdb = new MongoDb();
		testdb.on('connect', () => done());
		testdb.connect(testScaffold.config.uri);
	});

	it('should emit an error event on error', function(done) {
		let testdb = new MongoDb();
		testdb.on('connect', () => done(new Error('Unexpected success')));
		testdb.on('error', (err) => {
			expect(err).to.be.an.instanceof(MongoError);
			done();
		});
		testdb.connect(testScaffold.config.nonexistantUri);
	});
});
