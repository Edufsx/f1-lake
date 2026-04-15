# Cria imagem docker com python 3.13
FROM python:3.13

# Define diretório de trabalho
WORKDIR /apps/flask_api

# Copia arquivo de requirements dentro da imagem
COPY requirements.txt .

# Instala todos os requisitos dentro da imagem
RUN pip install --no-cache-dir -r requirements.txt

# Copia esses arquivos para o diretório de trabalho dentro do container
COPY . .

# Executa o script main.py
CMD [ "python", "main.py"]