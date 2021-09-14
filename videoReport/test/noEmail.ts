import { config } from 'dotenv';
import { handler } from '../src/index';

// Load env vars from .env
config({ path: __dirname + '/../../.env' });

// set report dealers with settings e.g. "bdc52a27-09d9-0384-f719-5744655473c3": { optInCodeText: true }
const dealerIds = {
    "bdc52a27-09d9-0384-f719-5744655473c3": {},
    "1f6f005005af11e888ba126446333acf": {},
};

// Invoke the lambda entrypoint for Parker Audi and Audi North Park
handler({ dealerIDs: dealerIds })
    .then(resp => console.log('Lambda fn completed with response:', resp));