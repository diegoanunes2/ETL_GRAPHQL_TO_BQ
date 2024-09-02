import query_list as query
import functions as fn
import column_type as ct
from datetime import  datetime, timedelta
import json
import pandas as pd
import sys

#=========================== Datas ===========================#

data_atual = datetime.now()                                               # Data atual
data_subtraida = data_atual - timedelta(days=500) # manter em 4   500 dias = 12000 h                        # Subtrair N dias de uma data
start = data_subtraida.replace(hour=0, minute=0, second=0,microsecond=0)  # Data inicial = data subtraida + mascara para 00:00
end = datetime.now().replace(hour=23, minute=59, second=59,microsecond=0) # Data final = data atual + mascara para 23:59
start_str = start.strftime("%d/%m/%Y %H:%M:%S")                           # Data inicial formatada para string (para uso na api)
end_str = end.strftime("%d/%m/%Y %H:%M:%S")
data_atual_formatada = '{}/{}/{}, {}:{}'.format(data_atual.day, data_atual.month, data_atual.year, data_atual.hour, data_atual.minute)


# =========================================== #
# === Ler os dados do arquivo config.ini  === #
# =========================================== #

# Chamar a função para ler as configurações
configuracoes = fn.ler_configuracoes('config.ini')

if configuracoes:
    # Sessão [config_api]: Obtém as configurações de acesso a API do arquivo config.ini
    url_api = configuracoes['url_api']
    token_api = configuracoes['token_api']
    token_bq = configuracoes['token_bq']
else:
    sys.exit(1)

## Lê o conteudo do arquivo Token

with open(token_api, "r") as arquivo :
	token = arquivo.read()
     
##================= exemplo ==================  
try:
    Dados_json = json.dumps(fn.executar_com_repeticao(
    token = token, 
    url = url_api,
    query = query.query_exemplo,
    endpoint_name = "exemplo", 
    max_tentativas=3, 
    intervalo_entre_tentativas=30,
    start_date=start_str,
    end_date=end_str))
    #Carrega arquivos como JSON
    Json_string_exemplo = json.loads(Dados_json)
    #=============== Cria Dataframe  ===============#
    df_exemplo = pd.json_normalize(Json_string_exemplo,sep='_')

    print(Dados_json)
except Exception as e:
    if 'data' in str(e):
        # Tratar a exceção quando a mensagem contém 'data'
        print('Não há dados a serem extraídos (!)')
    else:
        print(f'Ocorreu uma exceção: {e}')


#=============== Cria Dataframe  ===============#
df = pd.json_normalize(Json_string_exemplo,sep='_')

#========= Remove caracteres das colunas  ======#
fn.remove_char_columns(df)

#=====================(Integer)====================
for coluna in ct.exemplo_int:
 fn.adjust_type_integer(column=coluna, df=df)

#======= Envia os Dataframes Ajustados para o Bigquery =======#
fn.send_to_bigquery(table = 'caminho_no_bq.tabela_exemplo', dados = df, cert='GBQ.json') #
#======= Roda Consulta incremental (caso exista)
consulta_incremental_documentos = pd.read_gbq(credentials = vr.credentials, query = query.documentos_incremental)      
