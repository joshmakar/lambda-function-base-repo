import { config } from 'dotenv'
import { handler } from '../src/index'

// Load env vars from .env
config()

// Invoke the lambda entrypoint
handler({
    dealerIDs: [
        // Lexus of las vegas
        "00287d3e85e711e8841e121e8c516a7c",
        // Toyota of Portland
        "023d52cb-1317-4a75-baca-eb7eb8ebfb9b",
    ],
    emailRecipients: ["one@example.com"],
    startDate: "2021-05-18",
    endDate: "2021-06-18"
}).then(resp => console.log('Lambda fn completed with response:', resp))