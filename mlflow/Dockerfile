# Cria imagem docker com python 3.13
FROM python:3.13

# Define diretório de trabalho
WORKDIR /mlflow

# Instala todos os requisitos dentro da imagem
RUN pip install mlflow

# Sobe o servidor MLflow
CMD mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri sqlite:///mlflow.db \
    --default-artifact-root /mlflow/mlartifacts \
    --allowed-hosts "*"