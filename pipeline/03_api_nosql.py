#Exemplo salvando em noSQl utilizado tinydb
import requests
from tinydb import TinyDB
from datetime import datetime
import time

def extrair():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    return response.json()

def transformar(dados_json):
    valor = dados_json['data']['amount']
    criptomoeda = dados_json['data']['base']
    moeda = dados_json['data']['currency']
    dados_tratados = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": datetime.now().isoformat()
                }
    return dados_tratados

def load(dados_tratados):
    db = TinyDB('db.json')
    db.insert(dados_tratados)
    print("Dados salvos com sucesso!")

#while True + time.sleep(5) = looping para a ETL rodar (faz o get na API) automaticamente a cada 30 segundos sem o AirFlow

if __name__ == "__main__":
    while True:
        dados_json = extrair()
        dados_tratados = transformar(dados_json)
        load(dados_tratados)
        time.sleep(30)