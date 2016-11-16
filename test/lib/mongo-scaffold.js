// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

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
function connect(options = {}) {
	return mongo.connect(config.uri, options);
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

function close() {
	return Promise.resolve()
		.then(() => mongo.db.close());
}


module.exports = {
	config,
	reset,
	connect,
	resetAndConnect,
	close
};
