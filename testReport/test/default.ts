import { config } from 'dotenv';
import { handler } from '../src/index';

// Load env vars from .env
config({ path: __dirname + '/../../.env' });

handler({})
    .then(resp => console.log('Lambda fn completed with response:', resp));