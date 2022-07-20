import { handler } from './index';

/**
* Local testing
*/
const startDate = new Date('2021-12-01 00:00:00');
const endDate = new Date('2021-12-31 23:59:59');

const event = {
  Records: [
    {
      body: JSON.stringify({
        dealershipIntegralinkCodes: ['61540'],
        startDate: startDate,
        endDate: endDate,
        replyTo: 'https://webhook.site/74251db4-e0ee-4151-9ab5-33d998362a01',
      })
    }
  ]
};

handler(event, {}, (error: any, response: any) => {
  return response ? console.log('Response:', response) : console.log('Error:', error);
});
