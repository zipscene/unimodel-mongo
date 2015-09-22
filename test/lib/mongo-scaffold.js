/**
 * Common test code for Unimongo.
 */
const unimongo = require('../../lib');

const config = {
	uri: 'mongodb://localhost/unimongotest',
	nonexistantUri: 'mongodb://localhost:44332/nonexistant'
};

/**
 * Resets test environment back to neutral state.
 *
 * @method reset
 */
function reset() {
	return Promise.resolve()
		// Close any open database connections
		.then(() => unimongo.db.close())
		// Connect freshly to drop the database
		.then(connect)
		// Drop the database
		.then(() => unimongo.db.dropDatabase())
		// Close the connection
		.then(() => unimongo.db.close());
}

/**
 * Connects the default database instance to the test database.
 *
 * @method connect
 * @return {Promise} - Resolves when successfully connected.
 */
function connect() {
	return unimongo.connect(config.uri);
}

/**
 * Resets the test environment and connects to the database.
 *
 * @method resetAndConnect
 * @return {Promise}
 */
function resetAndConnect() {
	return reset().then(connect);
}


module.exports = {
	config,
	reset,
	connect,
	resetAndConnect
};
