import { handler } from './index';

/**
* Local testing
*/
const startDate = new Date('2021-12-01 00:00:00');
const endDate = new Date('2021-12-31 23:59:59');
// const startDate = new Date();
// startDate.setFullYear(startDate.getFullYear() - 2);
// const endDate = new Date();
// const startDate = new Date();
// startDate.setDate(startDate.getDate() - 10);
// const endDate = new Date();

const event = {
  Records: [
    {
      body: JSON.stringify({
        // dealershipIntegralinkCodes: ['e108cd88-bea5-f4af-11ac-574465d1fd2f'],
        // dealershipIntegralinkCodes: ['c5930e0c-72d6-4cd4-bfdf-d74db1d0ce38'],
        dealershipIntegralinkCodes: ['61540'],
        // dealershipIntegralinkCodes: ['99999'],
        // dealershipIntegralinkCodes: ['e108cd88-bea5-f4af-11ac-574465d1fd2f', 'c5930e0c-72d6-4cd4-bfdf-d74db1d0ce38'],
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