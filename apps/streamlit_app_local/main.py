# %%
import streamlit as st
import pandas as pd 
import requests

# Classe simples para representar os pilotos (usada no multiselect)
class Driver:

    def __init__(self, driverid, driver_name):
        self.driverid = driverid
        self.driver_name = driver_name

# Padroniza cores das equipes para formato hexadecimal válido
def format_color(x):
    
    if not x:
        return "#ffffff"
    
    elif len(x)==7:
        return x.lower()
  
    else:
        return f"#{x}".lower()

# Cacheia as predições para evitar chamadas repetidas à API
@st.cache_resource(ttl="1d")
def get_predictions(data):
    
    # Extrai o ano a partir da data de referência
    data["year"] = pd.to_datetime(data["dt_ref"]).dt.year

    # Converte data para string (Compatibilidade com modelo)
    data["dt_ref"] = data["dt_ref"].astype(str)

    df = data.copy()

    # Preenche valores nulos com valor utilizado no treinamento
    cols_to_fill = df.columns[3:-3]
    df[cols_to_fill] = df[cols_to_fill].fillna(-10000)

    # Transforma os dados no formato esperado pela API
    payload = {"values": df.to_dict(orient='records')}

    # Realiza requisição para o serviço de predição (Flask API)
    resp = requests.post("http://flask_api:5001/predict", json=payload)
    resp_json = resp.json()['predictions'] 

    # Extrai a probabilidade da classe '1' (1 = piloto campeão)
    pred_1_map = {k: v['1'] for k, v in resp_json.items()} 

    # Associa a probabilidade ao dataframe original
    df = df.assign(prob_win = df["id"].map(pred_1_map))

    # Padroniza as cores das equipes em formato válido
    df['teamcolor'] = df['teamcolor'].apply(format_color)

    # Corrige inconsistências nos nomes dos pilotos e mantém um nome único por driverid 
    unique_drivers_name = (df[['driverid', 'dt_ref', 'fullname']]
                           .dropna()
                           .sort_values(by=['driverid', 'dt_ref'])
                           .drop_duplicates(subset='driverid',
                                            keep='first')
                           .drop('dt_ref', axis=1)
                           .rename(columns={'fullname': 'fullname_correct'})
    )
    
    # Adiciona o nome correto dos pilotos no dataframe
    df = df.merge(unique_drivers_name, on='driverid')

    return df

# Carrega base de dados para predição do modelo
data = pd.read_parquet("/data/data_to_predict/fs_f1_driver_all.parquet")

# Executa o pipeline de predição
df = get_predictions(data)

# Cria base única de pilotos para o filtro da interface
drivers_data = (
    df[['driverid', 'fullname_correct']]
    .sort_values(['driverid', 'fullname_correct'])
    .drop_duplicates(subset=['driverid'], keep='first')
    .dropna()
)

# Converte para objetos Driver a base única de pilotos (melhora UX no Streamlit)
drivers = [
    Driver(i['driverid'], i['fullname_correct']) 
    for i in drivers_data.to_dict(orient='records')
]

# Seleciona os 3 pilotos com maior probabilidade de serem campeões na última data disponível
most_prob = (
    df[df['dt_ref'] == df['dt_ref'].max()]
    .sort_values(by="prob_win", ascending=False)
    .head(3)
)

# Define seleção padrão com base nos pilotos mais prováveis de serem campões
drivers_default = [i for i in drivers if i.driverid in most_prob["driverid"].tolist()]

#%%
# Configuração da página da APP web
st.set_page_config(page_title="F1 Data App",
                   page_icon=":racing_car:",
                   layout="wide")

st.markdown(
    """
    ## F1 Data App :checkered_flag:
    
    Aplicação interativa para análise e predição da probabilidade de pilotos se tornarem 
    campeões de temporadas da Fórmula 1, utilizando modelos de Machine Learning.

    Repositório do código: [https://github.com/Edufsx/f1-lake](https://github.com/Edufsx/f1-lake)
    """
)

# Layout com duas colunas para filtros
col1, col2 = st.columns(2)

with col1:
    # Filtro de pilotos
    driver_selected = st.multiselect(
        "Pilotos",
        options=drivers,
        format_func=lambda x: x.driver_name,
        default=drivers_default,
    )

with col2:
    # Filtro de Temporada
    year_selected = st.multiselect(
        "Temporada",
        options=df['year'].unique(),
        default =df['year'].max()
    )

# Aplica filtros de piloto e temporada selecionados pelo usuário
data_filtered = df[df["driverid"].isin([i.driverid for i in driver_selected])]
data_filtered = data_filtered[data_filtered["year"].isin(year_selected)]

# Define cores dos pilotos com base na equipe mais recente
colors = (data_filtered[['fullname_correct', 'dt_ref', 'teamcolor']]
          .sort_values(by=['fullname_correct', 'dt_ref'], ascending=[True, False])
          .drop_duplicates(subset=['fullname_correct'], 
                           keep='first')
          ['teamcolor'].tolist())

# Tabela com dados estruturados para melhor visualização temporal
data_chart = (data_filtered.pivot_table(index='dt_ref', 
                                        columns='fullname_correct', 
                                        values='prob_win')
                                        .reset_index())

# Configuração de exibição das colunas (formato percentual)
column_config = {
    i: st.column_config.NumberColumn(
        i, format="percent"
    ) for i in data_chart.columns[1:]
}

# Cria abas para navegação entre gráfico e tabelas
graph, tables = st.tabs(["Gráfico", "Tabelas"])

with graph:
    
    # Gráfico de linha mostrando evolução da probabilidade ao longo do tempo
    st.line_chart(
        data_chart,
        x='dt_ref',
        y=data_chart.columns.tolist()[1:],
        x_label='Data da Corrida',
        y_label='Prob. Vitória Campeonato',
        color=colors
    )

with tables:
    
    st.markdown("Tabela utilizada no Gráfico")
    # Tabela agregada utilizada no gráfico
    st.dataframe(
        data_chart, 
        hide_index=True, 
        column_config=column_config
    )
    
    st.markdown("Analytical Base Table")
    # Base completa após filtros (visão analítica)
    st.dataframe(
        data_filtered,
        hide_index=True
    )