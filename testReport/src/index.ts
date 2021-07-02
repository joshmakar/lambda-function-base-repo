/**
 * This is the entry point for the Lambda function
 */
export async function handler(event: {}) {
    const response = {
        "statusCode": 200,
        "headers": {
            "my_header": "my_value"
        },
        "body": JSON.stringify({ message: 'Hello BUG' }),
        "isBase64Encoded": false
    };

    return response;
};
