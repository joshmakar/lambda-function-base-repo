export interface DealershipDBInfo {
  internalCode: string;
  dealerName: string;
  connection: {
    host: string;
    database: string;
    user: string;
    password: string;
  };
}
