import pandas as pd
import requests, os
#from google.cloud import storage


# APIS VIA ENDPOINTS
def get_base_atleta(rodada:int) -> dict:
    """
    Essa função nos retornará a base de atletas para determinada rodada
    """
    f = requests.get(f"https://api.cartolafc.globo.com/atletas/pontuados/{str(rodada)}")
    data = f.json()
    return data

def get_base_rodada(rodada_atual:int) -> dict:
    """
    Essa função nos retornará a base de atletas para determinada rodada
    """
    f = requests.get(f"https://api.globoesporte.globo.com/tabela/d1a37fa4-e948-43a6-ba53-ab24ab3a45b1/fase/fase-unica-campeonato-brasileiro-2021/rodada/{str(rodada_atual)}/jogos")
    data = f.json()
    return data

#AUXLIAR COLUNAS
def get_colunas_scout(data: dict) -> list:
    id_atletas = list(data['atletas'].keys())
    colunas_scouts = []
    for id in id_atletas:
        if data['atletas'][id]['scout']:  # Se existe scout
            for scout in data['atletas'][id]['scout'].keys():  # para cada scout do atleta
                if scout not in colunas_scouts:
                    colunas_scouts.append(scout)
    return colunas_scouts

def get_rodada_carregadas(arquivos_salvos:list) -> list:
    """
    Script auxiliar para a partir de uma lista de arquivos salvos, nos retornas quais rodadas ainda temos que trazer
    """
    rodadas_carregadas = []
    for arquivo in arquivos_salvos:
        ref = arquivo[:-4]
        rodada = ref.split("_")[-1]
        rodadas_carregadas.append(int(rodada))
    return rodadas_carregadas


#GERANDO DATAFRAMES AUXILIARES
def get_dataframe_atletas(rodada_atual: int) -> pd.DataFrame:
    # Where the magic begins

    # Definindo minhas colunas a serem utilizadas
    data_1_rodada = get_base_atleta(1)  # Estamos considerando que todos os scouts necessários estarão sendo mapeados desde a 1 rodada.
    colunas_scouts = get_colunas_scout(data_1_rodada)
    colunas_base = ["rodada", "apelido", 'atleta_id', 'posicao_id', "clube_id", "entrou_em_campo", "pontuacao"]
    base = dict()
    colunas_final = colunas_base + colunas_scouts
    for coluna in colunas_final:
        base[coluna] = []

    # Gerando para nossa rodada atual
    data = get_base_atleta(rodada_atual)
    id_atletas = list(data['atletas'].keys())
    for id in id_atletas:
        for coluna in colunas_final:
            if coluna == 'rodada':
                base['rodada'].append(rodada_atual)
            elif coluna == 'atleta_id':
                base['atleta_id'].append(id)
            elif coluna in colunas_scouts:
                if data['atletas'][id]['scout']:
                    if coluna in data['atletas'][id]['scout'].keys():
                        base[coluna].append(data['atletas'][id]['scout'][coluna])
                    else:
                        base[coluna].append(0)
                else:
                    base[coluna].append(0)
            else:
                base[coluna].append(data['atletas'][id][coluna])
    return pd.DataFrame(base)

def get_dataframe_clubes(rodada_inicial=1) -> pd.DataFrame:
    data = get_base_atleta(rodada_inicial)
    base = {'clube_id': [], 'clube_nome': []}

    lista_clubes = list(data['clubes'].keys())
    for clube in lista_clubes:
        for coluna in list(base.keys()):
            if coluna == 'clube_id':
                base[coluna].append(data['clubes'][clube]['id'])
            elif coluna == 'clube_nome':
                base[coluna].append(data['clubes'][clube]['nome'])
            else:
                base[coluna].append(data['clubes'][clube][coluna])

    return (pd.DataFrame(base))

def get_dataframe_posicoes(rodada_inicial=1) -> pd.DataFrame:
    data = get_base_atleta(rodada_inicial)
    base = {'posicao_id': [], 'posicao_nome': [], 'posicao_abreviacao': []}

    lista_posicoes = list(data['posicoes'].keys())
    for posicao in lista_posicoes:
        for coluna in list(base.keys()):
            if coluna == 'posicao_id':
                base[coluna].append(data['posicoes'][posicao]['id'])
            elif coluna == 'posicao_nome':
                base[coluna].append(data['posicoes'][posicao]['nome'])
            elif coluna == 'posicao_abreviacao':
                base[coluna].append(data['posicoes'][posicao]['abreviacao'])
            else:
                base[coluna].append(data['posicoes'][posicao][coluna])

    return (pd.DataFrame(base))


def get_dataframe_jogos(rodada_atual: int) -> pd.DataFrame:
    data = get_base_rodada(rodada_atual)
    base = {}

    colunas_inicial = ['data_realizacao', 'hora_realizacao', 'placar_oficial_visitante', 'placar_oficial_mandante']
    coluna_equipes_mandantes = ['mandante_id', 'mandante_nome']
    coluna_equipes_visitantes = ['visitante_id', 'visitante_nome']
    colunas_base_rodada = ['rodada', 'jogo_id'] + colunas_inicial + coluna_equipes_mandantes + coluna_equipes_visitantes

    for coluna in colunas_base_rodada:
        base[coluna] = []

    for i in range(10):
        base['rodada'].append(rodada_atual)
        for chave in list(data[i].keys()):
            if chave == 'id':
                base['jogo_id'].append(data[i][chave])
            elif chave in colunas_inicial:
                base[chave].append(data[i][chave])
            elif chave == 'equipes':
                base['mandante_id'].append(data[i][chave]['mandante']['id'])
                base['mandante_nome'].append(data[i][chave]['mandante']['nome_popular'])
                base['visitante_id'].append(data[i][chave]['visitante']['id'])
                base['visitante_nome'].append(data[i][chave]['visitante']['nome_popular'])
            # elif chave == 'sede':
            #   if data[i][chave]['nome_popular']
            #   base['sede_nome'].append(data[i][chave]['nome_popular'])

    return pd.DataFrame(base)

#GERANDO DATAFRAMES CONSOLIDADOS
def get_dataframe_rodada(rodada_atual:int) -> pd.DataFrame:
    df_atletas = get_dataframe_atletas(rodada_atual)
    df_clubes = get_dataframe_clubes()
    df_posicoes = get_dataframe_posicoes()
    df_aux = pd.merge(df_atletas,df_clubes,on='clube_id',how='left')
    df = pd.merge(df_aux,df_posicoes,on = 'posicao_id',how='left')
    return df

def get_dataframe_confrontos(rodada_atual: int) -> pd.DataFrame:
    df = get_dataframe_jogos(rodada_atual)

    df_visitante = pd.DataFrame()
    df_visitante['rodada'] = df['rodada']
    df_visitante['clube_id'] = df['visitante_id']
    df_visitante['clube_nome'] = df['visitante_nome']
    df_visitante['aversario_id'] = df['mandante_id']
    df_visitante['aversario_nome'] = df['mandante_nome']
    df_visitante['gols_pro'] = df['placar_oficial_visitante']
    df_visitante['gols_contra'] = df['placar_oficial_mandante']
    df_visitante['jogo_id'] = df['jogo_id']
    df_visitante['data_realizacao'] = df['data_realizacao']
    df_visitante['hora_realizacao'] = df['hora_realizacao']
    df_visitante['mandante'] = False

    df_mandante = pd.DataFrame()
    df_mandante['rodada'] = df['rodada']
    df_mandante['clube_id'] = df['mandante_id']
    df_mandante['clube_nome'] = df['mandante_nome']
    df_mandante['aversario_id'] = df['visitante_id']
    df_mandante['aversario_nome'] = df['visitante_nome']
    df_mandante['gols_pro'] = df['placar_oficial_mandante']
    df_mandante['gols_contra'] = df['placar_oficial_visitante']
    df_mandante['jogo_id'] = df['jogo_id']
    df_mandante['data_realizacao'] = df['data_realizacao']
    df_mandante['hora_realizacao'] = df['hora_realizacao']
    df_mandante['mandante'] = True

    df_confrontos = pd.concat([df_mandante, df_visitante], ignore_index=True)

    return df_confrontos


#Salvando Resultado Final
def saving_dataframe_cartola_results(path_data):
    arquivos_salvos_cartola = [file for file in os.listdir(path_data) if
                               file.endswith('csv') and file.startswith('Base_Cartola')]
    rodadas_carregadas_cartola = get_rodada_carregadas(arquivos_salvos_cartola)

    for rodada in range(1, 37):
        if rodada not in rodadas_carregadas_cartola:
            try:
                df = get_dataframe_rodada(rodada)
                df.to_csv(f'{path_data}\\Base_Cartola_2021_rodada_{rodada}.csv')
                print(f'Base cartola carregada para pasta: {path_data}, na rodada: {rodada}')
            except:
                print(f'Base cartola ainda não disponível em API para rodada: {rodada}')
        else:
            print(f'Base cartola já existente na pasta: {path_data}, na rodada: {rodada}')

def saving_dataframe_confrontos_results(path_data):

    for rodada in range(1, 37):
        try:
            df = get_dataframe_confrontos(rodada)
            df.to_csv(f'{path_data}\\Base_Confrontos_2021_rodada_{rodada}.csv')
            print(f'Base Confrontos carregada para pasta: {path_data}, na rodada: {rodada}')
        except:
            print(f'Base Confrontos ainda não disponível em API para rodada: {rodada}')



#Iremos só utilizar essa função posteriormente quando enviarmos para o cloud nossos arquivos ...
def upload_stringio_to_google_storage(bucket_name, stringio, destination_blob_name, fileformat='text/csv'):
    """
    ENVIA ARQUIVO PARA O STORAGE
    """
    storage_client = storage.Client()
    print('Upload to google storage')
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    stringio.seek(0)
    blob.upload_from_string(stringio.read(), fileformat)

def run():
    # Carregando tabelas ainda não foram carregadas
    path_data = str(f"{os.getcwd()}\\data")
    saving_dataframe_cartola_results(path_data)
    saving_dataframe_confrontos_results(path_data)

if __name__ == '__main__':
    run()



