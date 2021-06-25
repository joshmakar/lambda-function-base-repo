import { config } from 'dotenv'
import { handler } from '../src/index'

// Load env vars from .env
config({ path: __dirname + '/../../.env' })

// Invoke the lambda entrypoint for Parker Audi and Audi North Park
handler({
    dealerIDs: ["bdc52a27-09d9-0384-f719-5744655473c3", "1f6f005005af11e888ba126446333acf"],
    // emailRecipients: ["dale.smith@myshopmanager.com"],
    // startDate: "2021-06-23",
    // endDate: "2021-06-24"
}).then(resp => console.log('Lambda fn completed with response:', resp))