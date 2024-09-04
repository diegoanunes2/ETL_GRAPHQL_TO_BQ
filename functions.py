import numpy as np
from configparser import ConfigParser
import os
from retrying import retry
import pandas as pd
import pip._vendor.requests as requests
import time
from google.oauth2 import service_account
from googlechatbot import GoogleChatBot
# =============================================================#
# ========================= Funções ===========================#
# =============================================================#


def ler_configuracoes(arquivo_config):
    """Lê o arquivo de configuração e retorna um dicionário com as configurações.

    Args:
        arquivo_config (str): Caminho completo para o arquivo de configuração.

    Returns:
        dict: Dicionário com as configurações, ou None se ocorrer algum erro.
    """

    if not os.path.exists(arquivo_config):
        print(
            f"\n(!) O arquivo de configuração '{arquivo_config}' não foi encontrado.\nCertifique-se que ele se encontra na raiz do diretório desta aplicação")
        return None

    config = ConfigParser(interpolation=None)
    config.read(arquivo_config, encoding='utf-8')

    configuracoes = {}
    try:
        # Sessão [config_api]
        configuracoes['url_api'] = config.get(
            'config_api', 'url_api')
        configuracoes['token_api'] = config.get(
            'config_api', 'token_api')
        configuracoes['token_bq'] = config.get(
            'config_api', 'token_bq')
        configuracoes['caminho_bq'] = config.get(
            'config_api', 'caminho_bq')

    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
        print(f"(!) Erro ao ler o arquivo de configuração: {e}")
        return None

    return configuracoes


def is_timeout_error(exception):
    return isinstance(exception, requests.exceptions.Timeout)


def chamar_api(token, url, query, endpoint_name, start_date, end_date):

    try:
        print(f"Iniciando a Extracao do endpoint {endpoint_name}")
        hasNext = True
        p_temp = 1
        p = 1
        data_list = []
        while True:

            url = url  # Endereço do modulo da API
            # Aqui se passa a query, pode-se adicionar ou remover valores (estes valores são os informados na doc da API)
            querytemp = query
            query_final = querytemp.replace('$p', str(p)).replace(
                '$start', str(start_date)).replace('$end', str(end_date))
            # cabeçalhos da requisição + passagem de token
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + token
            }

            # tentar timeout=None
            response = requests.get(
                url=url, json={"query": query_final}, headers=headers)
            response_json = response.json()  # retorna um dicionario (objeto Python)
            p += 1
            p_temp = p - 1
            # Verifica se tem mais paginas
            hasNext = response_json['data'][f'{endpoint_name}']['pageInfo']['hasNext']
            # total de paginas retornado pela API
            totalPages = response_json['data'][f'{endpoint_name}']['pageInfo']['totalPages']
            current_page_data = response_json['data'][f'{endpoint_name}']['list']
            data_list.extend(current_page_data)

            print(
                f"\rExtraindo a Pagina {p_temp} / {totalPages} do endpoint {endpoint_name}", end="")

            if hasNext == False:
                break
        print("Concatenando Dados")
        return data_list
    except (requests.RequestException, ValueError) as e:
        print(f"Erro ao chamar a API: {e}")
        return None

# ========================================================#
# ================ Função envio google chat ==============#
# ========================================================#


def enviar_mensagem_google_chat(mensagem, webhook_url):
    # Crie uma instância do GoogleChatBot
    chat_bot = GoogleChatBot(webhook_url)
    chat_bot.send_text_message(mensagem)

# ==============================================================#
# = Função para tentar mais vezes caso a API esteja fora do ar =#
# ==============================================================#


def executar_com_repeticao(token, url, query, endpoint_name, max_tentativas, intervalo_entre_tentativas, start_date, end_date):
    tentativa = 1
    while tentativa <= max_tentativas:
        print(f"Tentativa {tentativa}...")
        resultado = chamar_api(
            token, url, query, endpoint_name, start_date, end_date)
        if resultado is not None:
            print("API chamada com sucesso.")
            return resultado

        print(
            f"Erro na tentativa {tentativa}. Tentando novamente em {intervalo_entre_tentativas} segundos...")
        time.sleep(intervalo_entre_tentativas)
        tentativa += 1

    print(
        f"Nao foi possivel obter dados da API apos {max_tentativas} tentativas.")
    return None

# ==============================================================#
# = Função para Envio ao Bigquery, passar table, dados e cert  =#
# ==============================================================#


def send_to_bigquery(table, dados, cert):
    if not dados.empty:
        # Remove linhas duplicadas
        dados = dados.drop_duplicates()
        key_path = cert  # Chave gerada na API bigquery
        credentials = service_account.Credentials.from_service_account_file(key_path, scopes=[
            "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive"])
        # Tabela de destino
        dados.to_gbq(credentials=credentials,
                     destination_table=table, if_exists='replace')
        print(f"Dataframe {table} enviados ao Bigquery")
    else:
        print(f"Dataframe {table} Vazio, não será enviado ao Bigquery")


# =============================================================#
# ===== Função para ajuste dos tipos de dados das colunas =====#
# ======================== (String) ===========================#
# =============================================================#

def adjust_type_string(column, df):
    if not df.empty:
        if column in df.columns:
            df[column] = df[column].astype("str", errors='ignore')
        else:
            df[column] = ''
            df[column] = df[column].astype("str", errors='ignore')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

# =============================================================#
# ===== Função para ajuste dos tipos de dados das colunas =====#
# ======================== (Integer) ==========================#
# =============================================================#


def adjust_type_integer(column, df):
    if not df.empty:
        if column in df.columns:
            # Substituir valores em branco por zero
            df[column].fillna(0, inplace=True)
            df[column] = df[column].astype("int64", errors='ignore')
        else:
            df[column] = np.nan
            df[column] = df[column].astype("int64", errors='ignore')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

# =============================================================#
# ===== Função para ajuste dos tipos de dados das colunas =====#
# ====================== (Timestamp) ==========================#
# =============================================================#


def adjust_type_timestamp(column, df):
    if not df.empty:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column], yearfirst=True, errors='coerce')
        else:
            df[column] = pd.NaT
            df[column] = pd.to_datetime(
                df[column], yearfirst=True, errors='coerce')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

# =============================================================#
# ===== Função para ajuste dos tipos de dados das colunas =====#
# ====================== (Timestamp Day First) ==========================#
# =============================================================#


def adjust_type_timestamp_df(column, df):
    if not df.empty:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column], dayfirst=True, errors='coerce')
        else:
            df[column] = pd.NaT
            df[column] = pd.to_datetime(
                df[column], dayfirst=True, errors='coerce')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

# =============================================================#
# ===== Função para ajuste dos tipos de dados das colunas =====#
# ======================== (Float) ============================#
# =============================================================#


def adjust_type_float(column, df):
    if not df.empty:
        if column in df.columns:
            # Substituir valores em branco por zero
            df[column].fillna(0.0, inplace=True)

            # Converter a coluna para tipo numérico
            df[column] = pd.to_numeric(df[column], errors='coerce')
        else:
            print(f"Coluna {column} não encontrada no dataframe.")
    else:
        print(f"Dataframe {df} vazio, não será ajustado.")


def adjust_type_float(column, df):
    if not df.empty:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='ignore')
        else:
            df[column] = NaN
            df[column] = pd.to_numeric(df[column], errors='ignore')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

# =============================================================#
# ===== Função para ajuste dos tipos de dados das colunas =====#
# ======================= (Boolean) ===========================#
# =============================================================#


def adjust_type_boolean(column, df):
    if not df.empty:
        if column in df.columns:
            df[column] = df[column].astype("bool", errors='raise')
        else:
            df[column] = NaN
            df[column] = df[column].astype("bool", errors='raise')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

# =============================================================#
# ========   Função para remover colunas duplicadas   =========#
# =============================================================#


def remove_unused_columns(columns, df):
    return df.drop(columns=columns, errors='ignore')

# ============================================================================#
# === Função para remover os caracteres nas colunas no dataframe despesas ==#
# ============================================================================#


def remove_char_columns(df):
    if df.empty == False:
        df.columns = df.columns.str.replace(' ', '_', regex=True)
        df.columns = df.columns.str.replace('[/,\,-,),(,<,>]', '_', regex=True)
        df.columns = df.columns.str.replace(
            '[!,@,#,$,%,&,*.º,ª]', '', regex=True)
        df.columns = df.columns.str.replace(
            '[ã,Ã,á,Á,â,Â,à,À]', 'a', regex=True)
        df.columns = df.columns.str.replace('[ê,Ê,é,É]', 'e', regex=True)
        df.columns = df.columns.str.replace('[ç,Ç]', 'c', regex=True)
        df.columns = df.columns.str.replace('[õ,Ô,ó,Ó]', 'o', regex=True)
        df.columns = df.columns.str.replace('[í,Í]', 'i', regex=True)
        df.columns = df.columns.str.replace('[ü,Ü,ú,Ú]', 'u', regex=True)
        df.columns = df.columns.str.lower()

        return df
    else:
        print("Dataframe Vazio, o ajuste nao sera efetuado")

# ===========================================#
# === Função para ajustar valores BR > US ===#
# ===========================================#


def converter_valor(df, coluna):
    if coluna not in df:
        print(f"A coluna '{coluna}' não existe no DataFrame.")
        return df

    df[coluna] = df[coluna].str.replace('.', '')
    df[coluna] = df[coluna].str.replace(',', '.')
    return df
