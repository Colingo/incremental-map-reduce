# incremental-map-reduce

[![build status][1]][2] [![dependency status][3]][4]

[![browser support][5]][6]

incremental map reduce function for mongodb

## Example

To run an incremental map reduce you have to have a raw collection
that you want to map reduce and a target collection where you
reduce the data into.

The raw collection must have some kind of timestamp so that we
can query it to only reduce values since the last timestamp.

The output into the reduced collection must also have some
kind of timestamp which we can use to query the reduced collection
for the latest document that was reduced.

As long as those two are upheld we can run an incremental map reduce
which means only re-reducing new data which allows for efficient
map reduces on large append only collections.

```js
var incrementalMapReduce = require("incremental-map-reduce")
var passback = require("callback-reduce/passback")
var mongo = require("mongodb")

// Get a mongo db in whatever way you prefer
var db = someMongoDb
/* A collection containing

{
    id: someId
    , count: numberOfItemsOrSomething
    , timestamp: lastUpdatedTime
}

*/
var rawCollection = db.collection("raw-data")
var reducedCollection = db.collection("reduced.some-data")

var result = incrementalMapReduce(rawCollection, {
    reducedCollection: reducedCollection
    , map: map
    , reduce: reduce
    , options: {
        out: {
            reduce: "reduced.some-data"
        }
        , finalize: finalize
    }
})

passback(result, function (err, collection) {
    /* result of rawCollection.mapReduce(...) */
})

function map() {
    /*global emit*/
    emit(this.id, this)
}

function reduce(key, values) {
    var result = {
        id: values[0].id
        , count: 0
        , timestamp: 0
    }

    values.forEach(function (value) {
        result.count += value.count

        if (value.timestamp > result.timestamp) {
            result.timestamp = value.timestamp
        }
    })

    return result
}

function finalize(key, value) {
    return {
        id: value.id
        , count: value.count
        , timestamp: value.timestamp
    }
}
```

## Installation

`npm install incremental-map-reduce`

## Contributors

 - Raynos

## MIT Licenced

  [1]: https://secure.travis-ci.org/Colingo/incremental-map-reduce.png
  [2]: http://travis-ci.org/Colingo/incremental-map-reduce
  [3]: http://david-dm.org/Colingo/incremental-map-reduce/status.png
  [4]: http://david-dm.org/Colingo/incremental-map-reduce
  [5]: http://ci.testling.com/Colingo/incremental-map-reduce.png
  [6]: http://ci.testling.com/Colingo/incremental-map-reduce
