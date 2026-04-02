# %%
import os 
import dotenv
import nekt
from tqdm import tqdm
from pyspark.sql import SparkSession

os.environ["OMP_NUM_THREADS"] = "2"

# %%
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
query_dates = """
SELECT DISTINCT DATE(date) AS dtRef
FROM f1_results
WHERE YEAR(date) = '{year}'
ORDER BY 1 DESC
"""

query = """
WITH
  results_until_date AS (
    SELECT 
      *
    FROM
      f1_results
    WHERE
      DATE(date) <= DATE('{date}')
    ORDER BY
      date DESC
),

drivers_selected AS (
  SELECT DISTINCT
    driverid
  FROM 
    results_until_date
  WHERE YEAR >= (
          SELECT
            MAX(YEAR) - 2
          FROM
            results_until_date
        )
),

tb_results AS (
  SELECT 
    t1.*
  FROM
    results_until_date AS t1

  INNER JOIN drivers_selected AS t2
  ON t1.driverid = t2.driverid

  ORDER BY
    YEAR
),

tb_life AS (
SELECT 
  driverid,

  COUNT(DISTINCT year) AS qtde_seasons,

  COUNT(*) AS qtde_sessions,
  SUM(CASE WHEN status='Finished' OR status LIKE '+%' THEN 1 ELSE 0 END) AS qtde_session_finished,

  SUM(CASE WHEN mode='Race' THEN 1 ELSE 0 END) AS qtde_race,
  SUM(CASE WHEN (status='Finished' OR status LIKE '+%') AND mode='Race' THEN 1 ELSE 0 END) AS qtde_session_finished_race,
 
  SUM(CASE WHEN mode='Sprint' THEN 1 ELSE 0 END) AS qtde_sprint,
  SUM(CASE WHEN (status='Finished' OR status LIKE '+%') AND mode='Sprint' THEN 1 ELSE 0 END) AS qtde_session_finished_sprint,

  SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS qtde_1pos,
  SUM(CASE WHEN position = 1 AND MODE = 'Race' THEN 1 ELSE 0 END) AS qtde_1pos_race,
  SUM(CASE WHEN position = 1 AND MODE = 'Sprint' THEN 1 ELSE 0 END) AS qtde_1pos_sprint,
  
  SUM(CASE WHEN position <= 3 THEN 1 ELSE 0 END) AS qtde_podios,
  SUM(CASE WHEN position <= 3 AND MODE='Race' THEN 1 ELSE 0 END) AS qtde_podios_race,
  SUM(CASE WHEN position <= 3 AND MODE='Sprint' THEN 1 ELSE 0 END) AS qtde_podios_sprint,
  
  SUM(CASE WHEN position <= 5 THEN 1 ELSE 0 END) AS qtde_top5,
  SUM(CASE WHEN position <= 5 AND MODE='Race' THEN 1 ELSE 0 END) AS qtde_top5_race,
  SUM(CASE WHEN position <= 5 AND MODE='Sprint' THEN 1 ELSE 0 END) AS qtde_top5_sprint,
  
  SUM(CASE WHEN gridposition <= 5 THEN 1 ELSE 0 END) AS qtde_grid_top5,
  SUM(CASE WHEN gridposition <= 5 AND MODE='Race' THEN 1 ELSE 0 END) AS qtde_grid_top5_race,
  SUM(CASE WHEN gridposition <= 5 AND MODE='Sprint' THEN 1 ELSE 0 END) AS qtde_grid_top5_sprint,
  
  SUM(points) AS qtde_points,
  SUM(CASE WHEN mode='Race' THEN points ELSE 0 END) AS qtde_points_race,
  SUM(CASE WHEN mode='Sprint' THEN points ELSE 0 END) AS qtde_points_sprint,

  AVG(gridposition) AS avg_gridposition,
  AVG(CASE WHEN mode='Race' THEN gridposition END) AS avg_gridposition_race,
  AVG(CASE WHEN mode='Sprint' THEN gridposition END) AS avg_gridposition_sprint,

  AVG(position) AS avg_position,
  AVG(CASE WHEN mode='Race' THEN position END) AS avg_position_race,
  AVG(CASE WHEN mode='Sprint' THEN position END) AS avg_position_sprint,

  SUM(CASE WHEN gridposition=1 THEN 1 ELSE 0 END) AS qtde_1_grid_position,
  SUM(CASE WHEN gridposition=1 AND mode='Race' THEN 1 ELSE 0 END) AS qtde_1_grid_position_race,
  SUM(CASE WHEN gridposition=1 AND mode='Sprint' THEN 1 ELSE 0 END) AS qtde_1_grid_position_sprint,

  SUM(CASE WHEN gridposition=1 AND position=1 THEN 1 ELSE 0 END) AS qtde_pole_win,
  SUM(CASE WHEN gridposition=1 AND position=1 AND mode='Race' THEN 1 ELSE 0 END) AS qtde_pole_win_race,
  SUM(CASE WHEN gridposition=1 AND position=1 AND mode='Sprint' THEN 1 ELSE 0 END) AS qtde_pole_win_sprint,

  SUM(CASE WHEN points > 0 THEN 1 ELSE 0 END) AS qtde_session_points,
  SUM(CASE WHEN points > 0 AND mode='Race' THEN 1 ELSE 0 END) AS qtde_session_points_race,
  SUM(CASE WHEN points > 0 AND mode='Sprint' THEN 1 ELSE 0 END) AS qtde_session_points_sprint,

  SUM(CASE WHEN position < gridposition THEN 1 ELSE 0 END) AS qtde_sessions_with_overtake,
  SUM(CASE WHEN mode='Race' AND position < gridposition THEN 1 ELSE 0 END) AS qtde_sessions_with_overtake_race,
  SUM(CASE WHEN mode='Sprint' AND position < gridposition THEN 1 ELSE 0 END) AS qtde_sessions_with_overtake_sprint,

  AVG(gridposition - position) AS avg_overtake,
  AVG(CASE WHEN mode='Race' THEN gridposition - position END) AS avg_overtake_race,
  AVG(CASE WHEN mode='Sprint' THEN gridposition - position END) AS avg_overtake_sprint
  
  FROM
    tb_results

  GROUP BY
    driverid

  ORDER BY 
    qtde_podios DESC
)

SELECT DATE('{date}') AS dtRef, 
       *
FROM tb_life
ORDER BY driverid
"""

(nekt.load_table(layer_name="Bronze", table_name="f1_results")
    .createOrReplaceTempView("f1_results"))

years = list(range(1991,2026))

for y in years:

  dates = spark.sql(query_dates.format(year=y)).toPandas()["dtRef"].astype(str).tolist()
  df_all =  spark.sql(query.format(date=dates.pop(0)))

  for dt in tqdm(dates):
    df_all = df_all.union(spark.sql(query.format(date=dt)))

 
  nekt.save_table(
    df=df_all,
    layer_name="Silver",
    table_name="fs_f1_driver_life",
    folder_name="f1"
  )
  del(df_all)