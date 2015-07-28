const SchemaModel = require('zs-unimodel').SchemaModel;

class UnimongoModel extends SchemaModel {

	constructor(collectionName, schema, db, options = {}) {
		super(schema, options);
		this.db = db;
		this.collectionName = collectionName;
		this.collectionPromise = this.initCollection();
	}

	initCollection() {
		return this.db.dbPromise.then((db) => {

		});
	}

	getName() {
		return this.collectionName;
	}

	find(query, options = {}) {

	}

}
