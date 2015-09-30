const MongoDb = require('./mongo-db');
const MongoModel = require('./mongo-model');
const MongoDocument = require('./mongo-document');
const MongoError = require('./mongo-error');
const XError = require('xerror');

// Create a default global db instance
let defaultDatabase = new MongoDb();

// Expose the default database and a connect method
exports.db = defaultDatabase;
exports.connect = function(uri, options = {}) {
	return defaultDatabase.connect(uri, options);
};

// Add a function similar to mongoose which registers and retrieves models
let modelRegistry = {};
exports.model = function(model) {
	if (model) {
		modelRegistry[model.getName()] = model;
	} else {
		let name = model;
		model = modelRegistry[name];
		if (!model) throw new XError(XError.INTERNAL_ERROR, 'Model not found: ' + name);
		return model;
	}
};

// Add a function to create models with the default database
exports.createModel = function(modelName, schema = {}, options = {}) {
	return new MongoModel(modelName, schema, defaultDatabase, options);
};

// Export the various useful classes
exports.MongoDb = MongoDb;
exports.MongoModel = MongoModel;
exports.MongoDocument = MongoDocument;
exports.MongoError = MongoError;
