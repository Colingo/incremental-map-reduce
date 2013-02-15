var test = require("tape")
var uuid = require("node-uuid")
var after = require("after")

var expand = require("reducers/expand")
var fold = require("reducers/fold")
var take = require("reducers/take")
var mongo = require("mongo-client")
var insert = require("mongo-client/insert")
var find = require("mongo-client/find")
var close = require("mongo-client/close")
var passback = require("callback-reduce/passback")

var incrementalMapReduce = require("../index")

var client = mongo("mongodb://localhost/incremental-map-reduce:test")
var reducedCollectionName = uuid()
var rawCollection = client(uuid())
var reducedCollection = client(reducedCollectionName)
var ts = Date.now()

test("can run incremental map reduce", function (assert) {
    var insertion = insert(rawCollection, [{
        id: "1"
        , count: 22
        , timestamp: ts + 1
    }, {
        id: "2"
        , count: 23
        , timestamp: ts + 2
    }, {
        id: "1"
        , count: 40
        , timestamp: ts + 3
    }])

    var mapReduce = expand(take(insertion, 1), function () {
        return incrementalMapReduce(rawCollection, {
            reducedCollection: reducedCollection
            , map: map
            , reduce: reduce
            , options: {
                out: {
                    reduce: reducedCollectionName
                }
                , finalize: finalize
            }
        })
    })

    var results = expand(mapReduce, function () {
        return find(reducedCollection, {})
    })

    passback(results, Array, function (err, results) {
        assert.ifError(err)

        assert.deepEqual(results, [{
            _id: "1"
            , value: {
                id: "1"
                , count: 62
                , timestamp: ts + 3
            }
        }, {
            _id: "2"
            , value: {
                id: "2"
                , count: 23
                , timestamp: ts + 2
            }
        }])

        assert.end()
    })
})

test("doesn't re-run old data", function (assert) {
    var insertion = insert(rawCollection, [{
        id: "1"
        , count: 100
        , timestamp: ts + 2
    }, {
        id: "2"
        , count: 50
        , timestamp: ts + 4
    }])

    var mapReduce = expand(take(insertion, 1), function () {
        return incrementalMapReduce(rawCollection, {
            reducedCollection: reducedCollection
            , map: map
            , reduce: reduce
            , options: {
                out: {
                    reduce: reducedCollectionName
                }
                , finalize: finalize
            }
        })
    })

    var results = expand(mapReduce, function () {
        return find(reducedCollection, {})
    })

    passback(results, Array, function (err, results) {
        assert.ifError(err)

        assert.deepEqual(results, [{
            _id: "1"
            , value: {
                id: "1"
                , count: 62
                , timestamp: ts + 3
            }
        }, {
            _id: "2"
            , value: {
                id: "2"
                , count: 73
                , timestamp: ts + 4
            }
        }])

        assert.end()
    })
})

test("cleanup", function (assert) {
    var done = after(2, function () {
        close(rawCollection)
        assert.end()
    })

    fold(rawCollection, function (col) {
        col.drop(done)
    })

    fold(reducedCollection, function (col) {
        col.drop(done)
    })
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
