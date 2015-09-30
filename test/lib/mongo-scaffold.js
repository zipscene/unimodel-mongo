/**
 * Common test code for Mongo.
 */
const mongo = require('../../lib');

const config = {
	uri: 'mongodb://localhost/mongotest',
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
		.then(() => mongo.db.close())
		// Connect freshly to drop the database
		.then(connect)
		// Drop the database
		.then(() => mongo.db.dropDatabase())
		// Close the connection
		.then(() => mongo.db.close());
}

/**
 * Connects the default database instance to the test database.
 *
 * @method connect
 * @return {Promise} - Resolves when successfully connected.
 */
function connect() {
	return mongo.connect(config.uri);
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
