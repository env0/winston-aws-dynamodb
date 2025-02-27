'use strict';

import util from 'util';
import winston from 'winston';
import dynamodbIntegration from './lib/dynamodb-integration.js';
import isEmpty from 'lodash.isempty';
import isError from 'lodash.iserror';
import { stringify, debug } from './lib/utils.js';

const defaultFlushTimeoutMs = 10_000;
// we chose that as we wish to keep the message size under 400KB, to avoid truncation, and it should be enough as a safety net
const maxMessageLength = 300_000;

const WinstonDynamoDB = function (options) {
  winston.Transport.call(this, options);
  this.level = options.level || 'info';
  this.name = options.name || 'DynamoDB';
  this.tableName = options.tableName;
  this.logStreamName = options.logStreamName;
  this.options = options;

  const messageFormatter = options.messageFormatter
    ? options.messageFormatter
    : function (log) {
        return [log.level, log.message].join(' - ');
      };
  this.formatMessage = options.jsonMessage ? stringify : messageFormatter;
  this.proxyServer = options.proxyServer;
  this.uploadRate = options.uploadRate || 2000;
  this.logEvents = [];
  this.errorHandler = options.errorHandler;

  if (options.dynamoDbClient) {
    this.dynamoDB = options.dynamoDbClient;
  } else {
    throw new Error("Pass configured DynamoDB client as 'dynamoDbClient' option");
  }

  debug('constructor finished');
};

util.inherits(WinstonDynamoDB, winston.Transport);

WinstonDynamoDB.prototype.log = function (info, callback) {
  debug('log (called by winston)', info);

  if (!isEmpty(info.message) || isError(info.message)) {
    this.add(info);
  }

  if (!/^uncaughtException: /.test(info.message)) {
    // do not wait, just return right away
    return callback(null, true);
  }

  debug('message not empty, proceeding');

  // clear interval and send logs immediately
  // as Winston is about to end the process
  clearInterval(this.intervalId);
  this.intervalId = null;
  this.submit(callback);
};

WinstonDynamoDB.prototype.createUploadInterval = function () {
  this.intervalId = setInterval(() => {
    this.submit();
  }, this.uploadRate);
};

WinstonDynamoDB.prototype.add = function (log) {
  debug('add log to queue', log);

  const { message: originalMessage } = log;

  if (isEmpty(originalMessage) || isError(originalMessage)) {
    this.logEvents.push({
      message: this.formatMessage(log),
      timestamp: process.hrtime.bigint(),
      rawMessage: log
    });
  } else if (originalMessage.length <= maxMessageLength) {
    this.logEvents.push({
      message: this.formatMessage(log),
      timestamp: process.hrtime.bigint(),
      rawMessage: log
    });

    if (this.logEvents.length >= dynamodbIntegration.MAX_BATCH_ITEM_NUM) {
      debug('Max items for batch reached - submitting and rescheduling interval');
      clearInterval(this.intervalId);
      this.createUploadInterval();
      this.submit();
    }
  } else {
    for (let i = 0; i < originalMessage.length; i += maxMessageLength) {
      let currentMessageSlice = originalMessage.slice(i, i + maxMessageLength);
      this.logEvents.push({
        message: this.formatMessage({ ...log, message: currentMessageSlice }),
        timestamp: process.hrtime.bigint(),
        rawMessage: log
      });

      debug(`Send each slice individually right away. current slice number:  ${i / maxMessageLength + 1}`);
      clearInterval(this.intervalId);
      this.createUploadInterval();
      this.submit();
    }
  }

  if (!this.intervalId) {
    debug('creating interval');
    this.createUploadInterval();
  }
};

WinstonDynamoDB.prototype.submit = function (callback) {
  const defaultCallback = err => {
    if (err) {
      debug('error during submit', err, true);
      this.errorHandler && this.errorHandler(err);
    }
  };
  callback = callback || defaultCallback;

  const streamName = typeof this.logStreamName === 'function' ? this.logStreamName() : this.logStreamName;

  if (isEmpty(this.logEvents)) {
    return callback();
  }

  dynamodbIntegration.upload(this.dynamoDB, this.tableName, streamName, this.logEvents, this.options, callback);
};

WinstonDynamoDB.prototype.kthxbye = function (callback) {
  clearInterval(this.intervalId);
  this.intervalId = null;
  this.flushTimeout = this.flushTimeout || Date.now() + defaultFlushTimeoutMs;

  this.submit(
    function (error) {
      if (error) return callback(error);
      if (isEmpty(this.logEvents)) return callback();
      if (Date.now() > this.flushTimeout)
        return callback(new Error('Timeout reached while waiting for logs to submit'));
      else setTimeout(this.kthxbye.bind(this, callback), 0);
    }.bind(this)
  );
};

winston.transports.DynamoDB = WinstonDynamoDB;

export default WinstonDynamoDB;
