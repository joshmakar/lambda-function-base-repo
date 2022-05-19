export interface Dealer {
  integralinkCode: string;
  watson: {
    apiKey: string|null;
    decryptedApiKey: string|null;
  };
  instance: {
    url: string|null;
    apiUsername: string|null;
    apiPassword: string|null;
    database: {
      name: string;
      user: string;
      password: string;
      databaseServer: {
        IP: string;
      };
    };
  };
}
