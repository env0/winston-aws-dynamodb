'use strict';

const util = require('util'),
      winston = require('winston'),
      AWS = require('aws-sdk'),
      dynamodbIntegration = require('./lib/dynamodb-integration'),
      isEmpty = require('lodash.isempty'),
      assign = require('lodash.assign'),
      isError = require('lodash.iserror'),
      stringify = require('./lib/utils').stringify,
      debug = require('./lib/utils').debug,
      defaultFlushTimeoutMs = 10000;

const ALLOWED_REGIONS = ['us-east-1', 'us-west-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-northeast-1', 'ap-northeast-2', 'ap-southeast-1', 'ap-southeast-2', 'sa-east-1'];

var WinstonDynamoDB = function (options) {
    winston.Transport.call(this, options);
    this.level = options.level || 'info';
    this.name = options.name || 'DynamoDB';
    this.tableName = options.tableName;
    this.logStreamName = options.logStreamName;
    this.options = options;

    const awsAccessKeyId = options.awsAccessKeyId;
    const awsSecretKey = options.awsSecretKey;
    const awsRegion = options.awsRegion;
    const messageFormatter = options.messageFormatter ? options.messageFormatter : function(log) {
        return [ log.level, log.message ].join(' - ')
    };
    this.formatMessage = options.jsonMessage ? stringify : messageFormatter;
    this.proxyServer = options.proxyServer;
    this.uploadRate = options.uploadRate || 2000;
    this.logEvents = [];
    this.errorHandler = options.errorHandler;

    if (options.dynamoDB) {
        this.dynamoDB = options.dynamoDB;
    } else {
        if (this.proxyServer) {
            AWS.config.update({
                httpOptions: {
                    agent: require('proxy-agent')(this.proxyServer)
                }
            });
        }

        var config = {};

        if (awsAccessKeyId && awsSecretKey && awsRegion) {
            config = {credentials: {accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretKey}, region: awsRegion};
        } else if (awsRegion && !awsAccessKeyId && !awsSecretKey) {
            // Amazon SDK will automatically pull access credentials
            // from IAM Role when running on EC2 but region still
            // needs to be configured
            config = { region: awsRegion };
        }

        if (options.awsOptions) {
            config = assign(config, options.awsOptions);
        }

        this.dynamoDB = new AWS.DynamoDB(config);
    }

    if (ALLOWED_REGIONS.indexOf(this.dynamoDB.config.region) < 0) {
        throw new Error("unavailable region given");
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

    debug('message not empty, proceeding')

    // clear interval and send logs immediately
    // as Winston is about to end the process
    clearInterval(this.intervalId);
    this.intervalId = null;
    this.submit(callback);
};

WinstonDynamoDB.prototype.createUploadInterval = function() {
    debug('creating interval');
    this.intervalId = setInterval(() => {
        this.submit();
    }, this.uploadRate);
}

WinstonDynamoDB.prototype.add = function(log) {
    debug('add log to queue', log);

    if (!isEmpty(log.message) || isError(log.message)) {
        this.logEvents.push({
            message: this.formatMessage(log),
            timestamp: process.hrtime.bigint()
        });
    }

    // When we reach maximum amount of items in batch, reschedule
    if (this.logEvents.length >= dynamodbIntegration.MAX_BATCH_ITEM_NUM) {
        clearInterval(this.intervalId);
        this.createUploadInterval();
        this.submit();
    }

    if (!this.intervalId) {
        this.createUploadInterval()
    }
};

WinstonDynamoDB.prototype.submit = function(callback) {
    const defaultCallback = (err) => {
        if (err) {
            debug('error during submit', err, true);
            this.errorHandler ? this.errorHandler(err) : console.error(err);
        }
    }

    const streamName = typeof this.logStreamName === 'function' ?
        this.logStreamName() : this.logStreamName;

    if (isEmpty(this.logEvents)) {
        return callback();
    }

    dynamodbIntegration.upload(
        this.dynamoDB,
        this.tableName,
        streamName,
        this.logEvents,
        this.options,
        callback ?? defaultCallback
    );
};

WinstonDynamoDB.prototype.kthxbye = function(callback) {
    clearInterval(this.intervalId);
    this.intervalId = null;
    this.flushTimeout = this.flushTimeout || (Date.now() + defaultFlushTimeoutMs);

    this.submit((function(error) {
        if (error) return callback(error);
        if (isEmpty(this.logEvents)) return callback();
        if (Date.now() > this.flushTimeout) return callback(new Error('Timeout reached while waiting for logs to submit'));
        else setTimeout(this.kthxbye.bind(this, callback), 0);
    }).bind(this));
};

winston.transports.DynamoDB = WinstonDynamoDB;

module.exports = WinstonDynamoDB;
