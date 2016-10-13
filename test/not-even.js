const mongo = require('../lib/index');
const config = {
	uri: 'mongodb://localhost/mongotest2'
};
const pasync = require('pasync');

let model = mongo.createModel('Testings', {
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

let doc = model.create({
	pointzu: [
		[ -80, 30 ],
		[ -85, 35 ]
	],
	brandId: 'makros'
});

mongo.connect(config.uri).then(() => {
	return doc.save();
}).then(() => {
	console.log('ALL OVER');
}).catch(pasync.abort);
