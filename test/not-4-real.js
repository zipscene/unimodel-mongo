const _ = require('lodash');
const chai = require('chai');
const XError = require('xerror');
const { expect } = chai;
const { MongoDocument, createModel } = require('../lib');
const testScaffold = require('./lib/mongo-scaffold');
const { map } = require('zs-common-schema');
const { createQuery, createUpdate } = require('zs-common-query');

chai.use(require('chai-as-promised'));

const keySort = (a, b) => {
	for (let key in a.key) {
		if (''+a.key[key] > ''+b.key[key] || typeof b.key[key] === 'undefined') return 1;
		if (''+a.key[key] < ''+b.key[key]) return -1;
	}
	return 0;
};

describe('NOT4REAL', function() {
	beforeEach(testScaffold.resetAndConnect);

	it.only('a thing', function() {
		let model = createModel('Testings', {
			pointzu: [ {
				type: 'geopoint',
				index: {
					indexType: 'geoHashed',
					step: {
						base: 0.001,
						multiplier: 5,
						stepNum: 4
					}
				}
			} ],
			strArr: {
				type: 'array',
				elements: {
					dudes: {
						type: String,
						index: true
					}
				}
			},
			brandId: String
		});
		model.index({ brandId: 1, 'pointzu.$': {
			indexType: 'geoHashed',
			step: {
				base: 0.001,
				multiplier: 5,
				stepNum: 4
			}
		} });
		console.log('STUFF');
		console.log(model.getIndexes());
		console.log(model._geoHashedIndexMapping);

		let doc = model.create({
			pointzu: [
				[ -80, 30 ],
				[ -85, 35 ]
			],
			brandId: 'makros'
		});
		return doc.save();
	});
});
