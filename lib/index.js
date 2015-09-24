const UnimongoDb = require('./unimongo-db');
const UnimongoModel = require('./unimongo-model');
const UnimongoDocument = require('./unimongo-document');
const UnimongoError = require('./unimongo-error');
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
	return new UnimongoModel(modelName, schema, defaultDatabase, options);
};

// Export the various useful classes
exports.UnimongoDb = UnimongoDb;
exports.UnimongoModel = UnimongoModel;
exports.UnimongoDocument = UnimongoDocument;
exports.UnimongoError = UnimongoError;
