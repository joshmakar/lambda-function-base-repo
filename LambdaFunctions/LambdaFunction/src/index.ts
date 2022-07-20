// Import dependencies
import mysql from 'promise-mysql';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createObjectCsvStringifier } from 'csv-writer';
import axios from 'axios';

// Import modules
import { generateS3Key } from './modules/AwsS3Helpers';

// Configure environment variables if not in production
if (process.env['NODE_ENV'] !== 'production') {
  require('dotenv').config();
}

/**
 * The main function that runs the entire process.
 */
export async function handler(event: any, _context: any, callback: any) {
  try {
    event = JSON.parse(event.Records[0].body);

    // const s3Key = generateS3Key('name-of-file', 'csv', { prependToPath: 'directory_name' });

    // This works when running via nodejs
    // const s3 = new S3Client({
    //   region: 'us-east-1', // The value here doesn't matter.
    //   endpoint: 'http://localhost:4566', // This is the localstack EDGE_PORT
    //   forcePathStyle: true
    // });

    // This works when running via lambda
    // const s3 = new S3Client({
    //   region: 'us-east-1', // The value here doesn't matter.
    //   // endpoint: 'http://172.17.0.2:4566', // This is the localstack EDGE_PORT
    //   forcePathStyle: true
    // });

    // Save the csv data to S3
    // await s3.send(new PutObjectCommand({
    //   Bucket: process.env['NAME_OF_BUCKET'],
    //   Key: s3Key,
    //   Body: csvData,
    //   ContentType: 'text/csv',
    // }));

    callback(null, {
      statusCode: 201,
      body: 'Whatever you want to return',
      headers: {
        'X-Custom-Header': 'ASDF'
      }
    });
  } catch (error) {
    callback(error);
  }
}
