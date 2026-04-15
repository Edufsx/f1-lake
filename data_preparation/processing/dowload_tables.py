#%%
import nekt
from pyspark.sql import SparkSession
import os
import shutil
import dotenv
#%%
# Carrega variáveis de ambiente (.env)
dotenv.load_dotenv()

# Token de autenticação para acesso à Nekt
NEKT_TOKEN = os.getenv("NEKT_TOKEN")
nekt.data_access_token = NEKT_TOKEN

#%%
# Inicializa o Spark e conecta com Bigquery para leitura de dados
spark = (
    SparkSession.builder
    .appName("bigquery-test")

    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
    )
    .getOrCreate()
)

# Estabelece a engine de execução do Nekt como Spark
nekt.engine = "spark"
#%%
# Carrega feature histórica dos pilotos localizada na nekt como view temporária
nekt.load_table(
    layer_name="Silver",
    table_name="fs_f1_driver_all",
).createOrReplaceTempView("fs_f1_driver_all")

#%%
# Carrega tabela com campeões históricos como view temporária
nekt.load_table(
    layer_name="Silver",
    table_name="f1_champions",
).createOrReplaceTempView("f1_champions")

#%%
# Consulta para criação da Analytical Base Table (ABT)
query_abt = """
WITH tb_abt AS (

  SELECT t1.*,
        COALESCE(t2.rankDriver, 0) AS flChampion
  FROM fs_f1_driver_all AS t1

  LEFT JOIN f1_champions AS t2
  ON t1.driverid = t2.driverid
  AND EXTRACT(YEAR FROM t1.dt_ref) = t2.year

  WHERE t1.dt_ref >= DATE('2000-01-01') 
  AND t1.dt_ref < DATE('2026-01-01')

)

SELECT *
FROM tb_abt
"""

# Executa a consulta da ABT e a transforma em DataFrame
df_abt = spark.sql(query_abt).toPandas()

# Salva a ABT em formato CSV
df_abt.to_csv("../../data/processed/abt_f1_drivers_champion.csv", 
          index=False,
          sep=";")

#%%
# Consulta da feature store contendo dados mais recentes para predição
query_fs_all = """
SELECT *
FROM fs_f1_driver_all
"""
# Executa a consulta com dados para predição
df_fs_all = spark.sql(query_fs_all)
#%%
# Cria arquivo no formato parquet em uma pasta temporária
(df_fs_all.write.mode("overwrite")
 .option("header", True)
 .option("sep", ";")
 .parquet("../../data/fs_f1_driver_all_tmp"))
#%%
# Caminho da pasta temporária e da pasta final
output_folder = "../../data/fs_f1_driver_all_tmp"
final_path = "../../data/data_to_predict/fs_f1_driver_all.parquet"

# Itera todos os arquivos dentro da pasta temporária
for file in os.listdir(output_folder):
    
    # Se o arquivo for .parquet ele é colocado na pasta final
    if file.endswith(".parquet"):
        shutil.move(
            os.path.join(output_folder, file),
            final_path
        )

# Remove a pasta temporária
shutil.rmtree(output_folder) 