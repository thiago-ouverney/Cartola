import pandas as pd
import requests, os


def get_base_atleta(rodada:int) -> dict:
    """
    Essa função nos retornará a base de atletas para determinada rodada
    """
    f = requests.get(f"https://api.cartolafc.globo.com/atletas/pontuados/{str(rodada)}")
    data = f.json()
    return data


# Gerando base como dicionário vazio a ser preenchido

# Gerando lista de possíveis scouts::::
def get_colunas_scout(data: dict) -> list:
    id_atletas = list(data['atletas'].keys())
    colunas_scouts = []
    for id in id_atletas:
        if data['atletas'][id]['scout']:  # Se existe scout
            for scout in data['atletas'][id]['scout'].keys():  # para cada scout do atleta
                if scout not in colunas_scouts:
                    colunas_scouts.append(scout)
    return colunas_scouts


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

def get_dataframe_rodada(rodada_atual:int) -> pd.DataFrame:
    df_atletas = get_dataframe_atletas(rodada_atual)
    df_clubes = get_dataframe_clubes()
    df_posicoes = get_dataframe_posicoes()
    df_aux = pd.merge(df_atletas,df_clubes,on='clube_id',how='left')
    df = pd.merge(df_aux,df_posicoes,on = 'posicao_id',how='left')
    return df

def salvando_rodada(rodada_atual:int,path:str):
    try:
        df = get_dataframe_rodada(rodada_atual)
        df.to_csv(f'{path}\\Base_Cartola_2021_rodada_{rodada_atual}.csv')
        print(f'Base carregada para pasta: {path}, na rodada: {rodada_atual}')
    except Exception as err:
        print(f"Não foi possível carregar tabela {rodada_atual}")



if __name__ == '__main__':

    path_data = str(f"{os.getcwd()}\\data")
    arquivos_salvos = [file for file in os.listdir(path_data) if file.endswith('csv')]
    rodadas_carregadas = []
    for arquivo in arquivos_salvos:
        ref = arquivo[:-4]
        rodada = ref.split("_")[-1]
        rodadas_carregadas.append(int(rodada))

    for rodada in range(1, 37):
        if rodada not in rodadas_carregadas:
            try:
                salvando_rodada(rodada, path_data)
            except Exception as err:
                print(err)
