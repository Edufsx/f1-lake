/*
Construção da Analytical Base Table (ABT) para treinamento 
de modelos de aprendizado de máquina com o objetivo
de atribuir a probabilidade de cada piloto ser campeão
*/

WITH tb_abt AS (

  SELECT t1.*,
         -- Variável alvo (target): 
         -- Indica se o piloto foi campeão (1) ou não (0) na temporada
         COALESCE(t2.rankDriver, 0) AS flChampion
  FROM `f1_lake_2_silver`.`fs_f1_driver_all` AS t1

  -- Tabela de campeões para criação da variável alvo
  LEFT JOIN `f1_lake_2_silver`.`f1_champions` AS t2
  ON t1.driverid = t2.driverid
  AND EXTRACT(YEAR FROM t1.dt_ref) = t2.year

  -- Filtro temporal da base de modelagem
  WHERE t1.dt_ref >= DATE('2000-01-01') 
  AND t1.dt_ref < DATE('2026-01-01')

)

-- Dataset final para treino e validação dos modelos
SELECT *
FROM tb_abt