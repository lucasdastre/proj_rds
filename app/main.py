from requests import Session #fazer login apenas uma vez
from requests.exceptions import ConnectionError, Timeout ,TooManyRedirects
from dotenv import load_dotenv #pra pega var ambiente
import json
import os  #pra pegar var embiente
from pprint import pprint
import schedule
import time
from psycopg2 import sql

load_dotenv()

url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

paramaters = {

        "symbol" : "BTC",
        "convert" : "BRL"
}


headers = {

"Acceps": "application/json",
"X-CMC_PRO_API_KEY" : os.getenv("CMC_API_KEY"), #obetendo chave do env.

}

session = Session()
session.headers.update(headers)

def cot_bitcoin():

    try:
        response = session.get(url = url , params= paramaters)
        data =json.loads(response.text)

        if 'data' in data and 'BTC' in data['data']:
            bitcoin_data = data["data"]["BTC"]
            brl_quote = bitcoin_data["quote"]["BRL"]

            # pprint(brl_quote)
            pprint(brl_quote["price"])
            pprint(brl_quote["last_updated"])
            pprint(brl_quote["volume_24h"])
            pprint(brl_quote["market_cap"])
        else:
            print('Erro de status na cotação do bitcoin')
            
    
    except (ConnectionError,Timeout,TooManyRedirects) as e:
        print(f"Erro de requisição {e}")


schedule.every(15).seconds.do(cot_bitcoin)

print("iniciando agendamento para consultar api de bitcoin")

while True:
    schedule.run_pending()
    time.sleep(1)