# zs-unimodel-mongo

Unimodel library for MongoDB.

## Installation

```shell
$ npm install --save zs-unimodel-mongo
```

## Basic Usage

In this section, we will walk through basic usage for the library.

Initiate the default connection to mongo:
```js
let mongo = require('zs-unimodel-mongo');
mongo.connect('mongodb://localhost/mongotest');
});
```

Create an MongoModel:
```js
let Animal = mongo.createModel(
  'Animal', // model/collection name
  { // common-schema specification
    animalId: { type: String, index: true, id: true },
    name: { type: String, index: true }
  }
);
```

Register the MongoModel with the default model registry:
```js
mongo.model('Animal', Animal);
```

Use the model registry for CRUD operations on the model:
```js
let Animal = mongo.model('Animal');
let animal = Animal.create({ animalId: 'dog-charles-barkley', name: 'Charles Barkley' });
animal.save().then(() => {/* after save! */});
```

## Components
For more information on each of these components, see the generated docs.

### MongoDb
MongoDb is a wrapper around a [mongodb.Db][0] instance, which ensures a connection is established before
allowing operations against the MongoDB server.

### MongoModel
A MongoModel is a wrapper around a [mongodb.Collection][1] instance, and provides the interface specified
in `unimodel.SchemaModel`.

### MongoDocument
A MongoDocument encapsulates the data that is stored inside MongoDB, and provides the interface specified
in `unimodel.SchemaDocument`.

### MongoError
This is an [XError][2] wrapper around [mongodb.Error][3] objects.

[0]: http://mongodb.github.io/node-mongodb-native/2.0/api/Db.html
[1]: http://mongodb.github.io/node-mongodb-native/2.0/api/Collection.html
[2]: https://github.com/crispy1989/node-xerror
[3]: http://mongodb.github.io/node-mongodb-native/2.0/api/MongoError.html

## Quirks

### Indexing Map Types
Mongo does directly support indexing map types. To alleviate this, UnimodelMongo implements hidden
fields holding map index information, which is stored in a serialized BSON format.

#### Schema/Index Conversion
Schema/Index conversion works for both non-compound and compound indices, as long as the compound index is accessing the same map.
For example the following:
```js
let Animal = mongo.createModel('Animal', {
  name: { type: String, index: true }
  siblingAges: commonSchema.map({}, { age: Number }),
  beds: commonSchema.map({}, {
    averageSleepTime: { type: Number, index: true },
    longestSleepTime: Number,
    shortestSleepTime: Number
  })
});
Animal.index({ 'beds.averageSleepTime': 1, 'beds.longestSleepTime': 1 });
let dog = Animal.create({
  name: 'Charles',
  beds: {
    Couch: { averageSleepTime: 30 }
  }
});
```
Will save the following raw data into Mongo:
```js
let rawDog = {
  name: 'Charles',
  beds: {
    Couch: { averageSleepTime: 30, longestSleepTiem: 55 }
  },
  '_mapidx_beds^averageSleepTime': [
    BSON.serialize([ 'Couch', 30 ]).toString()
  ],
  '_mapidx_beds^averageSleepTime^longestSleepTime': [
    BSON.serialize([ 'Couch', 30, 55 ]).toString()
  ]
};
```
But the following compound index would not be allowed, since it is across multiple maps:
```js
Animal.index({ 'siblingAges.age': 1, 'beds.averageSleepTime': 1 });
```

#### Query Conversion
For a query to properly convert, certain conditions must be met:
* Map field must be indexed
* Multiple maps/keys cannot be in the same block
* No invalid query operators
* Extra fields in the map cannot be queried along with the indexed fields

For non-compound indices, the following query operators will be properly converted:
* `$eq`
* `$lt`
* `$lte`
* `$gt`
* `$gte`
So, the following:
```js
let rawQuery = { 'beds.Couch.averageSleepTime': 30 };
```
Becomes:
```js
let query = { '_mapidx_beds^averageSleepTime': BSON.serialize([ 'Couch', 30 ]).toString() };
```

For compound indices, only `$eq` is allowed.
So, the following:
```js
let rawQuery = {
  'beds.Couch.averageSleepTime': 30
  'beds.Couch.longestSleepTime': 55
};
```
Becomes:
```js
let query = {
  '_mapidx_beds^averageSleepTime^longestSleepTime': BSON.serialize([ 'Couch', 30, 55 ]).toString()
};
```

But the following examples will not be converted without breaking up the components into separate
`$and` blocks:
```js
let fail1 = { // Map field must be indexed
  'beds.Couch.longestSleepTime': 55
};
let fail2 = { // Multiple maps/keys cannot be in the same block
  'beds.Couch.averageSleepTime': 30
  'beds.Floor.longestSleepTime': 55
};
let fail3 = { // No invalid query operators
  'beds.Couch.averageSleepTime': 30
  'beds.Floor.longestSleepTime': { $lt: 55 }
};
let fail4 = { // Extra fields in the map cannot be queried along with the indexed fields
  'beds.Couch.averageSleepTime': 30
  'beds.Couch.longestSleepTime': 55,
  'beds.Couch.shortestSleepTime': 5
};
```
