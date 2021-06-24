"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const dotenv_1 = require("dotenv");
const index_1 = require("../src/index");
// Load env vars from .env
dotenv_1.config({ path: __dirname + '/../../.env' });
// Invoke the lambda entrypoint
index_1.handler({
    dealerIDs: [
        // Lexus of las vegas
        "00287d3e85e711e8841e121e8c516a7c",
        // Toyota of Portland
        "023d52cb-1317-4a75-baca-eb7eb8ebfb9b",
    ],
    emailRecipients: ["one@example.com"],
    // startDate: "2021-06-23",
    // endDate: "2021-06-24"
}).then(resp => console.log('Lambda fn completed with response:', resp));
