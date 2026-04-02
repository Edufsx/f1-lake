#%%
import nekt
from pyspark.sql import SparkSession
import os
import shutil
#%%
nekt.data_access_token = "z5MY5qxzntV3t5bLXwmJrYF66dtItFNyeL11GU35unpyum2IjZIkFEOhW1LXZO0jo3247zSsFzk27T2cZiwJW1gkcUmy8EdEekdPukY4RrGPtWJVXjkMAD0kxXdlZLkG9AMsPE1QzI4s685LHTujHX7K9PYev6Slvmknv096XB2V3AIgl5AJslpVSokVJmmXz2igNQFauEoqZGcfvjhjP3L03CCdr3rdE9YluAIb62Eik1OvFaw75BDGxAiebVcy"
#%%
spark = (
    SparkSession.builder
    .appName("bigquery-test")

    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
    )
    .getOrCreate()
)

nekt.engine = "spark"
#%%
nekt.load_table(
    layer_name="Silver",
    table_name="fs_f1_driver_all",
).createOrReplaceTempView("fs_f1_driver_all")

nekt.load_table(
    layer_name="Silver",
    table_name="f1_champions",
).createOrReplaceTempView("f1_champions")

#%%
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


df_abt = spark.sql(query_abt).toPandas()

df_abt.to_csv("../data/abt_f1_drivers_champion.csv", 
          index=False,
          sep=";")

#%%
query_fs_all = """
SELECT *
FROM fs_f1_driver_all
"""

df_fs_all = spark.sql(query_fs_all)
#%%
(df_fs_all.write.mode("overwrite")
 .option("header", True)
 .option("sep", ";")
 .parquet("../data/fs_f1_driver_all_tmp"))
#%%
output_folder = "../data/fs_f1_driver_all_tmp"
final_path = "../data/data_to_predict/fs_f1_driver_all.parquet"

for file in os.listdir(output_folder):
    if file.endswith(".parquet"):
        shutil.move(
            os.path.join(output_folder, file),
            final_path
        )

# 3. remove a pasta temporária
shutil.rmtree(output_folder) 