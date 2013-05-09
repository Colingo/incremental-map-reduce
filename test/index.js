var test = require("tape")
var uuid = require("node-uuid")
var after = require("after")
var mongo = require("continuable-mongo")

var incrementalMapReduce = require("../index")

var client = mongo("mongodb://localhost/incremental-map-reduce:test")
var reducedCollectionName = uuid()
var rawCollection = client.collection(uuid())
var reducedCollection = client.collection(reducedCollectionName)
var ts = Date.now()

test("can run incremental map reduce", function (assert) {
    rawCollection.insert([{
        id: "1",
        count: 22,
        timestamp: ts + 1
    }, {
        id: "2",
        count: 23,
        timestamp: ts + 2
    }, {
        id: "1",
        count: 40,
        timestamp: ts + 3
    }], function (err, docs) {
        assert.ifError(err)
        assert.equal(docs.length, 3)

        incrementalMapReduce(rawCollection, {
            reducedCollection: reducedCollection,
            map: map,
            reduce: reduce,
            options: {
                out: {
                    reduce: reducedCollectionName
                },
                finalize: finalize
            }
        }, function (err, result) {
            assert.ifError(err)
            assert.ok(result)

            reducedCollection.find({}).toArray(function (err, list) {
                assert.ifError(err)

                assert.deepEqual(list, [{
                    _id: "1",
                    value: {
                        id: "1",
                        count: 62,
                        timestamp: ts + 3
                    }
                }, {
                    _id: "2",
                    value: {
                        id: "2",
                        count: 23,
                        timestamp: ts + 2
                    }
                }])

                assert.end()
            })
        })
    })
})

test("doesn't re-run old data", function (assert) {
    rawCollection.insert([{
        id: "1",
        count: 100,
        timestamp: ts + 2
    }, {
        id: "2",
        count: 50,
        timestamp: ts + 4
    }], function (err, results) {
        assert.ifError(err)
        assert.equal(results.length, 2)

        incrementalMapReduce(rawCollection, {
            reducedCollection: reducedCollection,
            map: map,
            reduce: reduce,
            options: {
                out: {
                    reduce: reducedCollectionName
                },
                finalize: finalize
            }
        }, function (err, result) {
            assert.ifError(err)
            assert.ok(result)

            reducedCollection.find({}).toArray(function (err, list) {
                assert.ifError(err)

                assert.deepEqual(list, [{
                    _id: "1",
                    value: {
                        id: "1",
                        count: 62,
                        timestamp: ts + 3
                    }
                }, {
                    _id: "2",
                    value: {
                        id: "2",
                        count: 73,
                        timestamp: ts + 4
                    }
                }])

                assert.end()
            })
        })
    })
})

test("cleanup", function (assert) {
    var done = after(2, function () {
        client.close(function (err) {
            assert.ifError(err)

            assert.end()
        })
    })

    rawCollection(function (err, col) {
        col.drop(done)
    })
    reducedCollection(function (err, col) {
        col.drop(done)
    })
})

function map() {
    /*global emit*/
    emit(this.id, this)
}

function reduce(key, values) {
    var result = {
        id: values[0].id,
        count: 0,
        timestamp: 0
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
        id: value.id,
        count: value.count,
        timestamp: value.timestamp
    }
}
