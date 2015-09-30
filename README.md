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
