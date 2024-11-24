const { BatchWriteItemCommand } = require('@aws-sdk/client-dynamodb');
var isUndefined = require('lodash.isundefined');

const LIMITS = {
    MAX_BATCH_SIZE_BYTES: 16000000,     // We leave some fudge factor here too. This shouldn't be reachable for 25 items, however.
}

// CloudWatch adds 26. DynamoDB doesn't, but we wanna add a constant size to each item
// This size should depend on the attribute length and non-message values saved in DB
const BASE_EVENT_SIZE_BYTES = 26;

const debug = require('./utils').debug;

const lib = { MAX_BATCH_ITEM_NUM: 25 };

const popEventsToSend = (logEvents, cb) => {
    var entryIndex = 0;
    var bytes = 0;
    while (entryIndex < Math.min(logEvents.length, lib.MAX_BATCH_ITEM_NUM)) {
        var ev = logEvents[entryIndex];
        // unit tests pass null elements
        var evSize = ev ? Buffer.byteLength(ev.message, 'utf8') + BASE_EVENT_SIZE_BYTES : 0;

        if (bytes + evSize > LIMITS.MAX_BATCH_SIZE_BYTES) break;
        bytes += evSize;
        entryIndex++;
    }

    return logEvents.splice(0, entryIndex);
}

const buildPayload = (tableName, streamName, events, additionalAttributesSchema ) => {
    var additionalAttributes = additionalAttributesSchema ? Object.entries(additionalAttributesSchema) : [];
    return {
      RequestItems: {
        [tableName]: events.map((event) => {
          const rawMessage = event.rawMessage;
          const extraValues = {};
          additionalAttributes.forEach(([key, dynamoValueType]) => {
            if (!isUndefined(rawMessage[key])) {
              extraValues[key] = {
                [dynamoValueType]: rawMessage[key].toString(),
              };
            }
          });

          return {
            PutRequest: {
              Item: {
                ...extraValues,
                id: {
                  S: streamName,
                },
                timestamp: {
                  N: event.timestamp.toString(),
                },
                message: {
                  S: event.message,
                },
              },
            },
          };
        }),
      },
    };
  };

lib.upload = function(dynamodbClient, tableName, streamName, logEvents, options, cb) {
    debug('upload', logEvents);

    const eventsToSend = popEventsToSend(logEvents, cb);
    const payload = buildPayload(tableName, streamName, eventsToSend, options.additionalAttributesSchema);

    debug('send to aws');

    dynamodbClient.send(new BatchWriteItemCommand(payload), function (err, data) {
        debug('sent to aws, err: ', err, ' data: ', data)
        if (err) {
            debug('error during batchWriteItem', err, true)
            retrySubmit(dynamodbClient, payload, 3, cb)
        } else {
            cb()
        }
    });
};

function retrySubmit(dynamodbClient, payload, times, cb) {
    debug('retrying to upload', times, 'more times')

    dynamodbClient.send(new BatchWriteItemCommand(payload), function(err) {
        if (err && times > 0) {
            retrySubmit(dynamodbClient, payload, times - 1, cb)
        } else {
            cb(err)
        }
    })
}

module.exports = lib;
