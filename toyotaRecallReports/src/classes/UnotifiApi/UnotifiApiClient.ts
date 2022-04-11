import axios from 'axios';
import { DealershipDBInfo } from './interfaces/DealershipDBInfo';
import { Dealer } from './interfaces/Dealer';

export class UnotifiApiClient {
  private token: string;
  private baseUrl = 'http://index.unotifi.com/';
  private dealersEndpoint = 'api/dealers';

  constructor(token: string){
    this.token = token;
  }

  /**
   * Get all dealers from the unotifi API
   * @returns All dealers info
   */
  async getDealers(): Promise<Dealer[]> {
    const dealers: Dealer[] = [];

    await axios.get(`${this.baseUrl}${this.dealersEndpoint}`, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Content-Type': 'application/json',
        },
        params: {
          token: this.token,
        }
      })
      .then((response) => {
        response.data.data.forEach((dealership: Dealer) => {
          dealers.push(dealership);
        });
      })
      .catch((error) => {
        throw new Error("Error getting dealers", error);
      });

    return dealers;
  }

  /**
   * Get dealership database info from the unotifi API
   * @param dealershipIntegralinkCodes The dealership integralink/internal codes, e.g. '12345'
   * @returns Dealership database info for the dealerships with the given integralink codes
   */
  async getDealershipsDBInfo(dealershipIntegralinkCodes: string[]): Promise<DealershipDBInfo[]> {
    const dealershipsConnections: DealershipDBInfo[] = [];

    const dealers: Dealer[] = await this.getDealers();

    dealers.forEach((dealer) => {
      if (dealershipIntegralinkCodes.includes(dealer.integralinkCode)) {
        dealershipsConnections.push({
          internalCode: dealer.integralinkCode,
          dealerName: `Dealership ${dealer.integralinkCode}`,
          connection: {
            host: dealer.instance.database.databaseServer.IP,
            database: dealer.instance.database.name,
            user: dealer.instance.database.user,
            password: dealer.instance.database.password,
          },
        });
      }
    });

    return dealershipsConnections;
  }
}
