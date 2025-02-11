import WinstonDynamoDB from './index.js';
import winston from 'winston';
import { DynamoDB } from '@aws-sdk/client-dynamodb';

const dynamoDbClient = new DynamoDB();

const getDynamoDBTransporter = ()  => {
    return new WinstonDynamoDB({
        tableName: `deployment-step-service-pr18164-logs-pr18164`,
        logStreamName: 'yossi-log-46',
        jsonMessage: true,
        uploadRate: 500,
        dynamoDbClient,
        additionalAttributesSchema: {}
      });
};

const sendToConsole = new winston.transports.Console({
    level: 'debug',
    format: winston.format['json']()
  });

const logger = winston.createLogger({transports: [sendToConsole, getDynamoDBTransporter()]});

for (let i = 0; i < 100000; i++) {
    logger.error(`yossi the king ${i} test`);
}