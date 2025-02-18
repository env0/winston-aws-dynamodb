import { BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import isUndefined from 'lodash.isundefined';
import isEmpty from 'lodash.isempty';
import { debug } from './utils.js';


const LIMITS = {
  MAX_EVENT_MSG_SIZE_BYTES: 400000,   // The real max size is 409,600, we leave some room for overhead on each message
  MAX_BATCH_SIZE_BYTES: 16000000,     // We leave some fudge factor here too. This shouldn't be reachable for 25 items, however.
}

// CloudWatch adds 26. DynamoDB doesn't, but we wanna add a constant size to each item
// This size should depend on the attribute length and non-message values saved in DB
const BASE_EVENT_SIZE_BYTES = 26;


const lib = { MAX_BATCH_ITEM_NUM: 25 };

const popEventsToSend = (logEvents, cb) => {
    var entryIndex = 0;
    var bytes = 0;
    while (entryIndex < Math.min(logEvents.length, lib.MAX_BATCH_ITEM_NUM)) {
        var ev = logEvents[entryIndex];
        // unit tests pass null elements
        var evSize = ev ? Buffer.byteLength(ev.message, 'utf8') + BASE_EVENT_SIZE_BYTES : 0;
        if(evSize > LIMITS.MAX_EVENT_MSG_SIZE_BYTES) {
            evSize = LIMITS.MAX_EVENT_MSG_SIZE_BYTES;
            ev.message = ev.message.substring(0, evSize);
            const msgTooBigErr = new Error('Message Truncated because it exceeds the DynamoDB size limit');
            msgTooBigErr.logEvent = ev;
            cb(msgTooBigErr);
        }
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

const hasUnprocessedItems = (data) => !isEmpty(data.UnprocessedItems);

const isDynamoDbError = (err, data) => err || hasUnprocessedItems(data);

const sendLogsToDynamoDb = (dynamodbClient, payload, cb, times = 3) => new Promise((resolve) => {
  dynamodbClient.send(new BatchWriteItemCommand(payload), function (err, data) {
    debug('sent to aws, err: ', err, ' data: ', data)
    if (isDynamoDbError(err, data) && times > 0) {
      debug('error during batchWriteItem', err, true)

      if (hasUnprocessedItems(data)) {
        retrySubmit(dynamodbClient, {RequestItems: data.UnprocessedItems}, cb, times - 1)
      } else {
        retrySubmit(dynamodbClient, payload, cb, times - 1)
      }
    } else {
      cb()
    }

    resolve();
  });
})

lib.upload = function(dynamodbClient, tableName, streamName, logEvents, options, cb) {
    debug('upload', logEvents);

    const eventsToSend = popEventsToSend(logEvents, cb);
    const payload = buildPayload(tableName, streamName, eventsToSend, options.additionalAttributesSchema);

    debug('send to aws');

    sendLogsToDynamoDb(dynamodbClient, payload, cb);
};

function retrySubmit(dynamodbClient, payload, cb, times) {
    debug('retrying to upload', times, 'more times')

    sendLogsToDynamoDb(dynamodbClient, payload, cb, times);
}

export default lib;
