"use strict";

var util = require('util'),
    winston = require('winston'),
    AWS = require('aws-sdk'),
    dynamodbIntegration = require('./lib/dynamodb-integration'),
    isEmpty = require('lodash.isempty'),
    isError = require('lodash.iserror'),
    stringify = require('./lib/utils').stringify,
    debug = require('./lib/utils').debug,
    defaultFlushTimeoutMs = 10000;

const ALLOWED_REGIONS = ['us-east-1', 'us-west-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-northeast-1', 'ap-northeast-2', 'ap-southeast-1', 'ap-southeast-2', 'sa-east-1'];

var WinstonDynamoDB = (function (options) {
    winston.Transport.call(this, options);
    this.level = options.level || 'info';
    this.name = 'DynamoDB';
    this.tableName = options.tableName;
    this.logStreamName = options.logStreamName;
    this.uploadRate = options.uploadRate || 2000;
    this.logEvents = [];
    this.errorHandler = options.errorHandler;

    var awsAccessKeyId = options.awsAccessKeyId;
    var awsSecretKey = options.awsSecretKey;
    var awsRegion = options.awsRegion;
    var messageFormatter = options.messageFormatter ? options.messageFormatter : function(log) {
        return [ log.level, log.message ].join(' - ')
    };
    this.formatMessage = options.jsonMessage ? stringify : messageFormatter;

    if (options.dynamoDB) {
        this.dynamoDB = options.dynamoDB;
    } else {
        var config = {};
        if (awsAccessKeyId && awsSecretKey && awsRegion) {
            config = {credentials: {accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretKey}, region: awsRegion};
        }

        this.dynamoDB = new AWS.DynamoDB(config);
    }

    if (ALLOWED_REGIONS.indexOf(this.dynamoDB.config.region) < 0) {
        throw new Error("unavailable region given");
    }

    debug('constructor finished');
});

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

WinstonDynamoDB.prototype.add = function(log) {
    debug('add log to queue', log);

    var self = this;

    if (!isEmpty(log.message) || isError(log.message)) {
        self.logEvents.push({
            message: self.formatMessage(log),
            timestamp: new Date().getTime()
        });
    }

    if (!self.intervalId) {
        debug('creating interval');
        self.intervalId = setInterval(function() {
            self.submit(function(err) {
                if (err) {
                    debug('error during submit', err, true);
                    self.errorHandler ? self.errorHandler(err) : console.error(err);
                }
            });
        }, self.uploadRate);
    }
};

WinstonDynamoDB.prototype.submit = function(callback) {
    var streamName = typeof this.logStreamName === 'function' ?
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
        callback
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

winston.transports.CloudWatch = WinstonDynamoDB;

module.exports = WinstonDynamoDB;
