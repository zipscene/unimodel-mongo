const commonQuery = require('common-query');

exports.queryFactory = commonQuery.defaultQueryFactory;

exports.updateFactory = new commonQuery.UpdateFactory();
let setOnInsert = new commonQuery.coreUpdateOperators.UpdateOperatorSet('$setOnInsert');
exports.updateFactory.registerUpdateOperator('$setOnInsert', setOnInsert);

exports.aggregateFactory = commonQuery.defaultAggregateFactory;

