const SchemaModel = require('zs-unimodel').SchemaModel;
const objtools = require('zs-objtools');

class UnimongoModel extends SchemaModel {

	constructor(collectionName, schema, db, options = {}) {
		// Initialize superclass
		super(schema, options);

		// Set class variables
		this.db = db;
		this.dbPromise = db.dbPromise;
		this.collectionName = collectionName;

		// Translate options
		this.options = options;

		// Map from our option names to mongo collection options
		let collectionOptionMap = {
			w: 'w',
			writeConcern: 'w',
			wtimeout: 'wtimeout',
			writeConcernTimeout: 'wtimeout',
			j: 'j',
			journalWriteConcern: 'j',
			raw: 'raw',
			pkFactory: 'pkFactory',
			readPreference: 'readPreference',
			serializeFunctions: 'serializeFunctions'
		};
		this.collectionOptions = {};
		for (let opt in collectionOptionMap) {
			if (opt in options) {
				this.collectionOptions[collectionOptionMap[opt]] = options[opt];
			}
		}

		// Start initializing the collection
		this.collectionPromise = this.initCollection();
	}

	_ensureExists() {
		this.dbPromise.then((db) => {
			db.collection(
				this.collectionName,
				objtools.merge({}, this.collectionOptions, { strict: true }),
				function(err, collection) {
					console.log(err);
				}
			);
		});
	}

	_ensureIndices() {

	}

	initCollection() {
		return this._ensureExists()
			.then(() => this._ensureIndices());
	}

	getName() {
		return this.collectionName;
	}

	find(query, options = {}) {

	}

}
