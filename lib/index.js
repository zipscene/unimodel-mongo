// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const CursorResultStream = require('./cursor-result-stream');
const MongoDb = require('./mongo-db');
const MongoModel = require('./mongo-model');
const MongoDocument = require('./mongo-document');
const MongoError = require('./mongo-error');

// Create a default global db instance
let defaultDatabase = new MongoDb();

// Expose the default database and its connect, model, and createModel methods
exports.db = defaultDatabase;
exports.connect = function(uri, options = {}) {
	return defaultDatabase.connect(uri, options);
};
exports.model = function(model) {
	return defaultDatabase.model(model);
};
exports.createModel = function(modelName, schema, options) {
	return defaultDatabase.createModel(modelName, schema, options);
};

// Export the various useful classes
exports.CursorResultStream = CursorResultStream;
exports.MongoDb = MongoDb;
exports.MongoModel = MongoModel;
exports.MongoDocument = MongoDocument;
exports.MongoError = MongoError;
