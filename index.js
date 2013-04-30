var findOne = require("mongo-client/findOne")
var mapReduce = require("mongo-client/mapReduce")
var expand = require("reducers/expand")

module.exports = incrementalMapReduce

function incrementalMapReduce(col, opts) {
    var timestampPath = opts.timestampPath || "timestamp"
    var lastTimestampPath = opts.lastTimestampPath || "timestamp"

    var lastValue = findOne(opts.reducedCollection, {}, {
        sort: [["value." + lastTimestampPath, -1]],
        batchSize: 10
    })

    var result = expand(lastValue, function (doc) {
        var lastTimestamp = doc && doc.value && doc.value[lastTimestampPath]
        var options = opts.options || {}

        if (lastTimestamp) {
            options.query = options.query || {}
            options.query[timestampPath] = {
                $gt: lastTimestamp
            }
        }

        return mapReduce(col, opts.map, opts.reduce, options)
    })

    return result
}
