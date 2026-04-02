# %%
import streamlit as st
import pandas as pd 
import requests

class Driver:
    
    def __init__(self, driverid, driver_name):
        self.driverid = driverid
        self.driver_name = driver_name

def format_color(x):
    
    if not x:
        return "#ffffff"
    
    elif len(x)==7:
        return x.lower()
  
    else:
        return f"#{x}".lower()

@st.cache_resource(ttl="1d")
def get_predictions(data):
    
    data["year"] = pd.to_datetime(data["dt_ref"]).dt.year

    data["dt_ref"] = data["dt_ref"].astype(str)

    df = data.copy()

    cols_to_fill = df.columns[3:-3]
    df[cols_to_fill] = df[cols_to_fill].fillna(-10000)

    payload = {"values": df.to_dict(orient='records')}

    resp = requests.post("http://127.0.0.1:5001/predict", json=payload)
    resp_json = resp.json()['predictions'] 

    pred_1_map = {k: v['1'] for k, v in resp_json.items()}   
    df = df.assign(predictions = df["id"].map(pred_1_map))

    df['teamcolor'] = df['teamcolor'].apply(format_color)

    df = df.sort_values(by="dt_ref").copy()

    return df

data = pd.read_parquet("../data/data_to_predict/fs_f1_driver_all.parquet")

df = get_predictions(data)

drivers_data = (
    data[['driverid', 'fullname']]
    .sort_values(['driverid', 'fullname'])
    .drop_duplicates(subset=['driverid'], keep='first')
    .dropna()
)

drivers = [
    Driver(i['driverid'], i['fullname']) 
    for i in drivers_data.to_dict(orient='records')
]

most_prob = (
    df[df['dt_ref'] == df['dt_ref'].max()]
    .sort_values(by="predictions", ascending=False)
    .head(3)
)

#%%
st.set_page_config(page_title="F1 Data App",
                   page_icon=":racing_car:",
                   layout="wide")

st.markdown("## F1 Data App")

col1, col2 = st.columns(2)

drivers_default = [i for i in drivers if i.driverid in most_prob["driverid"].tolist()]

driver_selected = col1.multiselect(
    "Pilotos",
    options=drivers,
    format_func=lambda x: x.driver_name,
    default=drivers_default,
)

year_selected = col2.multiselect(
    "Temporada",
    options=df['year'].unique(),
    default =df['year'].max()
)

data_filtered = df[df["driverid"].isin([i.driverid for i in driver_selected])]
data_filtered = data_filtered[data_filtered["year"].isin(year_selected)]

colors = (data_filtered[['fullname', 'dt_ref', 'teamcolor']]
          .sort_values(by=['fullname', 'dt_ref'], ascending=[True, False])
          .drop_duplicates(subset=['fullname'], 
                           keep='first')
          ['teamcolor'].tolist())

data_chart = (data_filtered.pivot_table(index='dt_ref', 
                                      columns='fullname', 
                                      values='predictions')
                                      .reset_index())

graph, tables = st.tabs(["Gráfico", "Tabelas"])

with graph:
    st.line_chart(data_chart,
                x='dt_ref',
                y=data_chart.columns.tolist()[1:],
                x_label='Data da Corrida',
                y_label='Prob. Vitória Campeonato',
                color=colors
    )

 
with tables:
    st.markdown("Tabela referente ao Gráfico")
    st.dataframe(data_chart, hide_index=True)
    
    st.markdown("Analytical Base Table")
    st.dataframe(df, hide_index=True)