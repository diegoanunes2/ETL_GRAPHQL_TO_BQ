#=============================================================#
#========================= Variaveis =========================#
#=============================================================#

from datetime import date, datetime, timedelta
import time                                                        # usada para funções de data hora etc
from google.cloud import bigquery                                  # Requisito para envios BigQuery
from google.oauth2 import service_account                          # Requisito para envios BigQuery

#=========================== Datas ===========================#

data_atual = datetime.now()                                               # Data atual
data_subtraida = data_atual - timedelta(hours=3)                          # Subtrair N dias de uma data
start = data_subtraida.replace(hour=0, minute=0, second=0,microsecond=0)  # Data inicial = data subtraida + mascara para 00:00
end = datetime.now().replace(hour=23, minute=59, second=59,microsecond=0) # Data final = data atual + mascara para 23:59
start_str = start.strftime("%d/%m/%Y %H:%M:%S")                           # Data inicial formatada para string (para uso na api)
end_str = end.strftime("%d/%m/%Y %H:%M:%S")
data_atual_formatada = '{}/{}/{}, {}:{}'.format(data_atual.day, data_atual.month, data_atual.year, data_atual.hour, data_atual.minute)


#===== URLs dos Endpoints  ====#
url_exemplo = 'https://app.exemplo/api/endpoint/exemplo'


#==== Arquivos de autenticação ====#
cert_gbq = "GBQ.json"                   # Caminho do Json do Big query Server
token_path ="Token/token.txt"
credentials = service_account.Credentials.from_service_account_file(filename=cert_gbq,scopes=["https://www.googleapis.com/auth/cloud-platform"])

#==== Chat para Alertas ====#
webhook_url = "https://chat.googleapis.com/v1/spaces/..." 



#============= Localização das tabelas no Destino (Incrementais)=================#

exemplo_bq = 'fcomex_exportacao_incr.contentores'
