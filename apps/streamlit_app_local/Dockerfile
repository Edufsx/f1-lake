# Cria imagem docker com python 3.13
FROM python:3.13

# Define diretório de trabalho
WORKDIR /apps/streamlit_app_local

# Copia arquivo de requirements dentro da imagem
COPY requirements.txt .

# Instala todos os requisitos dentro da imagem
RUN pip install -r requirements.txt 

# Copia todos os arquivos para o diretório de trabalho dentro do container
COPY . .

# Executa comando streamlit run main.py
CMD [ "streamlit", "run", "main.py" ]