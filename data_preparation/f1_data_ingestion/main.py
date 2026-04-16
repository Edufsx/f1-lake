#%%
from collect import CollectResults
from sender import Sender
import datetime
import os
import dotenv
import time

# Carrega variáveis ambiente
dotenv.load_dotenv()

# Nome do bucket S3 onde os dados serão armazenados
BUCKET_NAME = os.getenv("BUCKET_NAME")

#%%

# Loop contínuo para coleta e envio periódico de dados da Fórmula 1
while True:
    
    print("Iniciando processo...")

    print("Coletando dados...")
    
    # Coleta dados da temporada atual da Fórmula 1 
    collect_data = CollectResults(
        years=[datetime.datetime.now().year]
    )
    collect_data.process_years()

    print("Enviando dados...")

    # Envia os dados coletados para o bucket S3 
    sender_data = Sender(
        bucket_name=BUCKET_NAME, 
        bucket_folder="f1/results"
    )
    sender_data.process_folder("data/raw")

    print("Iteração Finalizada")
    
    # Aguarda 7 dias antes da próxima execução
    time.sleep(60*60*24*7)