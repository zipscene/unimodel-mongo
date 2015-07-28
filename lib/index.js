const UnimongoDb = require('./unimongo-db');
const UnimongoModel = require('./unimongo-model');
const XError = require('xerror');

// Create a default global db instance
let defaultDatabase = new UnimongoDb();

// Expose the default database and a connect method
exports.db = defaultDatabase;
exports.connect = function(uri, options = {}) {
	return defaultDatabase.connect(uri, options);
};

// Add a function similar to mongoose which registers and retrieves models
let modelRegistry = {};
exports.model = function(name, model) {
	if (model) {
		modelRegistry[name] = model;
	} else {
		model = modelRegistry[model];
		if (!model) throw new XError(XError.INTERNAL_ERROR, 'Model not found: ' + name);
		return model;
	}
};

// Add a function to create models with the default database
exports.createModel = function(collectionName, schema, options = {}) {
	return new UnimongoModel(collectionName, schema, defaultDatabase, options);
};

// Export the various useful classes
exports.UnimongoDb = UnimongoDb;
exports.UnimongoModel = UnimongoModel;
