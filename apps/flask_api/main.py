#%%
import flask 
import mlflow
import pandas as pd
import dotenv
import os

# Carrega as variáveis contidas no arquivo .env
dotenv.load_dotenv()

# Define o endereço do servidor do MLflow
mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))

# Busca versões registradas do modelo em produção
versions = mlflow.search_model_versions(filter_string="name='f1_driver_champion'")

# Identifica a versão mais recente do modelo
last_version = max([int(i.version) for i in versions])

# Carrega a última versão registrada do modelo
MODEL = mlflow.sklearn.load_model(f"models:/f1_driver_champion/{last_version}")

#%%
# Inicializa a aplicação Flask
app =  flask.Flask(__name__)

# Endpoint para monitoramento da API
@app.route('/health_check')
def health_check():
    return 'OK', 200

# Endpoint para predição da probabilidade dos pilotos serem campeões
@app.route('/predict', methods=['POST'])

def predict():

    # Extrai os dados enviados na requisição
    payload = flask.request.get_json()
    data = payload.get('values', [])
    
    # Validação dos dados extraídos 
    if len(data) == 0:
        return {"error": "No features provided"}, 400

    # Converte os dados para DataFrame
    df = pd.DataFrame(data)

    # Garante que as features estejam alinhadas com o treinamento do modelo
    X = df[MODEL.feature_names_in_]
    
    # Gera as probabilidades de cada classe
    df_proba = pd.DataFrame(MODEL.predict_proba(X), columns=MODEL.classes_)

    # Adiciona o ID do piloto as probabilidades
    df_proba["id"] = df["id"].copy()
    df_proba.set_index("id", inplace=True)

    # Converte o resultado das predições para o formato JSON
    payload = df_proba.to_dict(orient='index')

    return{"predictions" : payload}, 200

# Ponto de entrada para execução local da aplicação
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
