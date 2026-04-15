-- Consulta para encontrar o campeão de cada temporada da Fórmula 1

WITH
    -- Agrega a pontuação de cada piloto por temporada
    year_driver_points AS (

        SELECT CAST(year AS INT) AS year,
               driverId,
               SUM(points) AS totalPoints
        FROM `f1_lake_2_bronze`.`f1_results`
        
        WHERE mode IN ('Race', 'Sprint')
        -- Exclui o ano atual, pois a temporada pode ainda estar em andamento
        AND year <> EXTRACT(year FROM CURRENT_DATE)
        
        GROUP BY year, driverId

    ),

    -- Aplica uma window function para ranquear os pilotos em cada temporada
    rn_year_driver AS (
        
        SELECT *,
               -- Define o ranking de pilotos dentro de cada ano (1 = maior pontuação)
               ROW_NUMBER() OVER (PARTITION BY YEAR ORDER BY totalPoints DESC) AS rankDriver
        FROM year_driver_points
    
    )

-- Seleciona apenas o campão de cada temporada da Fórmula 1
SELECT *  
FROM rn_year_driver
WHERE rankDriver = 1