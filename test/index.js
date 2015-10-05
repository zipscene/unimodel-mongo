const expect = require('chai').expect;
const mongo = require('../lib/index');

describe('zs-unimodel-mongo', () => {
	it('should register and be able to get model', () => {
		let Collection = mongo.createModel('Collection', {
			id: { type: String, index: true, id: true },
			name: { type: String, index: true }
		});
		mongo.model(Collection);
		let GotModel = mongo.model('Collection');
		expect(GotModel).to.be.instanceOf(mongo.MongoModel);
		expect(GotModel.getName()).to.equal('Collection');
	});

	it('should throw error when passed in non model instance nor string', () => {
		try {
			mongo.model(1);
		} catch (ex) {
			expect(ex.code).to.equal('invalid_argument');
			expect(ex.message).to.equal('argument must either be a string or a model instance');
		}
	});

	it('should return error when trying to get non-existing model', () => {
		try {
			mongo.model('NonExist');
		} catch (ex) {
			expect(ex.code).to.equal('internal_error');
			expect(ex.message).to.equal('Model not found: NonExist');
		}
	});
});