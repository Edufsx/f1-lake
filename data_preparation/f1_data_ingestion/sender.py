#%%
import argparse
import dotenv
import os
import boto3
from tqdm import tqdm

# Carrega as variáveis do arquivo .env 
dotenv.load_dotenv()

# Carregas as chaves de acesso da AWS
AWS_KEY = os.getenv("AWS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

#%%
class Sender:
    """
    Classe responsável por enviar arquivos no formato .parquet para um bucket S3 na AWS.

     A classe permite:
    - Conectar ao serviço S3 utilizando credenciais armazenadas em variáveis de ambiente.
    - Fazer upload de arquivos individuais para um diretório específico no bucket.
    - Processar uma pasta inteira, enviando todos os arquivos .parquet.
    - Remover os arquivos locais após upload bem-sucedido (evitando duplicidade).

    Parâmetros:
    -----------
    - `bucket_name`: str -> Nome do bucket S3 de destino;

    - `bucket_folder`: str -> Caminho dentro do bucket onde os arquivos serão armazenados.
    """

    def __init__(self, bucket_name, bucket_folder):
        
        self.bucket_name = bucket_name
        self.bucket_folder = bucket_folder

        # Criação do cliente S3 com autenticação via variáveis ambiente
        self.s3 = boto3.client(
            "s3", 
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="us-east-2"
        )   

    def process_file(self, filename):
        """
        Realiza o upload de um arquivo único para o S3.

        Etapas:
        - Extrai o nome do arquivo a partir do caminho completo;
        - Define o caminho de destino no bucket;
        - Faz o upload;
        - Remove o arquivo local após upload com sucesso
 
        Parâmetros:
        -----------
        - `filename`: str -> Caminho completo do arquivo local
        
        Retorna:
        --------
        - bool -> True se o upload for bem-sucedido, False caso contrário.
        """

        # Define o caminho final dentro do bucket
        file = filename.split("/")[-1]
        bucket_path = f"{self.bucket_folder}/{file}"

        try:
            # Upload do arquivo para o S3
            self.s3.upload_file(
                filename, 
                self.bucket_name, 
                bucket_path
            )
        
        except Exception as err:
            print(err)
            return False

        # Remove o arquivo local após upload bem-sucedido
        os.remove(filename)

        return True
    
    def process_folder(self, folder):
        """
        Realiza o upload para o S3 de todos os arquivos .parquet 
        de uma pasta local.

        Para cada arquivo:
        - Verifica se possui extensão .parquet;
        - Realiza o upload para o S3.

        Parâmetros:
        -----------
        - folder: str -> nome da pasta local 
        """

        # Cria uma lista com os arquivos .parquet da pasta
        files = [i for i in os.listdir(folder) if i.endswith(".parquet")]
        
        # Itera a lista criada com barra de progresso
        for f in tqdm(files):
            self.process_file(f"{folder}/{f}")
            
#%%
# --- Interface de linha de comando (CLI) para execução do pipeline de upload de dados. ---
"""
Permite ao usuário definir:
- Nome do bucket S3 de destino (via --bucket)
- Caminho do bucket (via --bucket_path)
- Pasta contendo arquivo .parquet a serem enviados (via --folder)
"""

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    # Argumento obrigatório: nome do bucket
    parser.add_argument("--bucket", type=str)
    
    parser.add_argument("--bucket_path", type=str)
    parser.add_argument("--folder", default="../../data/raw", type=str)
    
    args = parser.parse_args()
    
    # Executa apenas se o bucket for informado
    if args.bucket:
        send = Sender(args.bucket, args.bucket_path)
        send.process_folder(args.folder)

    else:
        print("Sem Bucket definido")