module.exports = incrementalMapReduce

function incrementalMapReduce(col, opts, callback) {
    var timestampPath = opts.timestampPath || "timestamp"
    var lastTimestampPath = opts.lastTimestampPath || "timestamp"

    opts.reducedCollection.findOne({}, {
        sort: [["value." + lastTimestampPath, -1]],
        batchSize: 10
    }, function (err, lastValue) {
        if (err) {
            return callback(err)
        }

        var lastTimestamp = lastValue && lastValue.value &&
            lastValue.value[lastTimestampPath]
        var options = opts.options || {}

        if (lastTimestamp) {
            options.query = options.query || {}
            options.query[timestampPath] = {
                $gt: lastTimestamp
            }
        }

        col.mapReduce(opts.map, opts.reduce, options, callback)
    })
}
