from requests import Session #fazer login apenas uma vez
from requests.exceptions import ConnectionError, Timeout ,TooManyRedirects
from dotenv import load_dotenv #pra pega var ambiente
import json
import os  #pra pegar var embiente
from pprint import pprint
import schedule
import time
from psycopg2 import sql
import csv
import psycopg2
from psycopg2 import OperationalError

load_dotenv()

DB_HOST= os.getenv('DB_HOST')
DB_NAME= os.getenv('DB_NAME')
DB_USER= os.getenv('DB_USER')
DB_PASS= os.getenv('DB_PASS')


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


def rds_connection():
    try:
        # Estabelecendo a conexão com o RDS PostgreSQL
        connection = psycopg2.connect(
            host= DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port= 5432
        )
        print("Conexão ao RDS PostgreSQL bem-sucedida!")
        return connection
    
    except OperationalError as e:
        print(f"Ocorreu um erro ao conectar ao RDS: {e}")
        return None


def criar_tabela(connection):
    try:
        conn = rds_connection()
        cursor = conn.cursor()
        
        # Criação da tabela
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS bitcoin_quotes (
            id SERIAL PRIMARY KEY,
            price NUMERIC,
            volume_24h NUMERIC,
            market_cap NUMERIC,
            last_updated TIMESTAMP
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Tabela criada ou já existente.")
    except Exception as e:
        print(f"Erro ao criar a tabela: {e}")

# Função para salvar os dados no banco de dados
def salvar_no_rds(usd_quote):
    try:
        conn = rds_connection()
        cursor = conn.cursor()
        
        # Inserção dos dados na tabela
        insert_query = sql.SQL(
            "INSERT INTO bitcoin_quotes (price, volume_24h, market_cap, last_updated) VALUES (%s, %s, %s, %s)"
        )
        cursor.execute(insert_query, (
            usd_quote['price'],
            usd_quote['volume_24h'],
            usd_quote['market_cap'],
            usd_quote['last_updated']
        ))
        conn.commit()
        cursor.close()
        conn.close()
        print("Dados salvos com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar dados no RDS: {e}")


def cot_bitcoin():

    try:
        response = session.get(url = url , params= paramaters)
        data =json.loads(response.text)

        if 'data' in data and 'BTC' in data['data']:
            bitcoin_data = data["data"]["BTC"]
            brl_quote = bitcoin_data["quote"]["BRL"]

            salvar_no_rds(brl_quote)
        
        else:
            print('Erro de status na cotação do bitcoin')
            
    
    except (ConnectionError,Timeout,TooManyRedirects) as e:
        print(f"Erro de requisição {e}")



criar_tabela(rds_connection())


schedule.every(15).seconds.do(cot_bitcoin)

print("iniciando agendamento para consultar api de bitcoin")

while True:
    schedule.run_pending()
    time.sleep(1)
