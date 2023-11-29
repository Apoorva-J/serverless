import { Storage } from '@google-cloud/storage';
import axios from 'axios';
import Mailgun from 'mailgun.js';
import FormData from 'form-data';
const mailgun = new Mailgun(FormData);
import * as uuid from 'uuid';
import AWS from 'aws-sdk';

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const mg = mailgun.client({ username: "api", key: process.env.EMAIL_API_KEY });

const storage = new Storage({
    credentials: JSON.parse(process.env.GCP_SERVICE_ACCOUNT_PVT_KEY),
});

export const handler = async (event) => {
    if (event.Records && event.Records.length > 0 && event.Records[0].Sns) {
        const snsMessage = JSON.parse(event.Records[0].Sns.Message);
        const snsEmail=snsMessage.email
        const fileName = snsMessage.url.substring(snsMessage.url.lastIndexOf('/') + 1);
        const fileLocation = "Assignment - " + snsMessage.assignment_id + " / " + snsMessage.email + " / " + snsMessage.num_of_attempts + " / " + fileName;
        let dynamoDBParams = {
            TableName: process.env.DYNAMODB_TABLE_NAME,
            Item: {
                id: uuid.v4(),
                assignment_id: snsMessage.assignment_id,
                email: snsMessage.email,
                num_attempts: snsMessage.num_of_attempts,
                file_location: fileLocation,
                timestamp: new Date().toISOString(),
            },
        };
        const GCS_BUCKET = process.env.GCS_BUCKET_NAME;
        const url = snsMessage.url;
        const bucketObj = storage.bucket(GCS_BUCKET)
        const file = bucketObj.file(fileLocation);
        const fileConfig = await downloadFile(url);
        await file.save(fileConfig);
        mg.messages.create(process.env.EMAIL_DOMAIN, {
            from: `CSYE6225 <apoorva@${mailgunDomain}>`,
            to: [snsEmail],
            subject: "Assignment submission received",
            text: `Your submission was successfully received and verified at ${fileLocation}. Thank you.`,
        })
            .then(msg => console.log(msg))
            .catch(err => console.error(err));
        const res = {
            statusCode: 200,
            body: JSON.stringify('Lambda function successful'),
        };
        await dynamoDB.put(dynamoDBParams).promise();
        return res;
    } else {
        mg.messages.create(process.env.EMAIL_DOMAIN, {
            from: `CSYE6225 <apoorva@${mailgunDomain}>`,
            to: [snsEmail],
            subject: "Assignment submission failed",
            text: "Your submission could not be downloaded. Please verify the URL and resubmit.",
        })
            .then(msg => console.log(msg))
            .catch(err => console.error(err));
        const res = {
            statusCode: 400,
            body: JSON.stringify('Invalid event source'),
        };
        await dynamoDB.put(dynamoDBParams).promise();
        return res;
    }
};

async function downloadFile(url) {
    const res = await axios.get(url, { resType: 'arraybuffer' });
    return Buffer.from(res.data, 'binary');
}