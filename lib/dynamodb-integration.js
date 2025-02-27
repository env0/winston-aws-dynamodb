import { BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import isUndefined from 'lodash.isundefined';
import isEmpty from 'lodash.isempty';
import { debug } from './utils.js';

const LIMITS = {
  MAX_EVENT_MSG_SIZE_BYTES: 400000, // The real max size is 409,600, we leave some room for overhead on each message
  MAX_BATCH_SIZE_BYTES: 16000000, // We leave some fudge factor here too. This shouldn't be reachable for 25 items, however.
  MAX_BATCH_WRITING_RETRIES: 5
};

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
    if (evSize > LIMITS.MAX_EVENT_MSG_SIZE_BYTES) {
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
};

const buildPayload = (tableName, streamName, events, additionalAttributesSchema) => {
  var additionalAttributes = additionalAttributesSchema ? Object.entries(additionalAttributesSchema) : [];
  return {
    RequestItems: {
      [tableName]: events.map(event => {
        const rawMessage = event.rawMessage;
        const extraValues = {};
        additionalAttributes.forEach(([key, dynamoValueType]) => {
          if (!isUndefined(rawMessage[key])) {
            extraValues[key] = {
              [dynamoValueType]: rawMessage[key].toString()
            };
          }
        });

        return {
          PutRequest: {
            Item: {
              ...extraValues,
              id: {
                S: streamName
              },
              timestamp: {
                N: event.timestamp.toString()
              },
              message: {
                S: event.message
              }
            }
          }
        };
      })
    }
  };
};

const hasUnprocessedItems = data => !isEmpty(data?.UnprocessedItems);

const isDynamoDbError = (err, data) => err || hasUnprocessedItems(data);

const sleep = async timeoutMs => await new Promise(resolve => setTimeout(() => resolve(), timeoutMs));

const sendLogsToDynamoDb = (dynamodbClient, payload, cb, attempt = 1) =>
  new Promise(resolve => {
    dynamodbClient.send(new BatchWriteItemCommand(payload), async function (err, data) {
      debug('sent to aws, err: ', err, ' data: ', data);

      if (isDynamoDbError(err, data)) {
        debug('error during batchWriteItem', err, true);
        if (attempt > LIMITS.MAX_BATCH_WRITING_RETRIES) {
          debug(`Got to ${LIMITS.MAX_BATCH_WRITING_RETRIES} attempts, not retrying.`);
          cb();
          resolve();
        }

        // Wait before retry
        const backoffMs = 500 * Math.pow(2, attempt - 1);
        await sleep(backoffMs);

        // Retry according to issue
        if (hasUnprocessedItems(data)) {
          sendLogsToDynamoDb(dynamodbClient, { RequestItems: data.UnprocessedItems }, cb, attempt + 1);
        } else {
          sendLogsToDynamoDb(dynamodbClient, payload, cb, attempt + 1);
        }
      } else {
        cb();
      }

      resolve();
    });
  });

lib.upload = function (dynamodbClient, tableName, streamName, logEvents, options, cb) {
  debug('upload', logEvents);

  const eventsToSend = popEventsToSend(logEvents, cb);
  const payload = buildPayload(tableName, streamName, eventsToSend, options.additionalAttributesSchema);

  debug('send to aws');

  sendLogsToDynamoDb(dynamodbClient, payload, cb);
};

export default lib;
