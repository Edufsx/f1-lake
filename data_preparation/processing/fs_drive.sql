/* 
Feature Store por piloto (driverid) e data de referência (dt_ref),
considerando janelas temporais parametrizáveis ({last_races}):
- Todas as corridas do piloto;
- As 40 últimas corridas do piloto;
- AS 20 últimas corridas do piloto;
- As 10 últimas corridas do piloto;
*/

WITH
	-- Determina todas as datas de referência únicas dentro de um intervalo
	dim_dates AS (
	
		SELECT DISTINCT 
				DATE(date) AS dt_ref,
				year AS ref_year
		FROM f1_results
		
		-- Intervalo parametrizável 
		WHERE year >= {year_start} AND year <= {year_stop}
	
	),

	-- Para cada data de referência, recupera todas as sessões passadas (<= dt_ref)
	past_sessions AS (
	
		SELECT t1.dt_ref, 
			   t1.ref_year,
			   t2.*
		FROM dim_dates AS t1
	
		INNER JOIN f1_results AS t2
		ON DATE(t2.date) <= t1.dt_ref
	
	),

	-- Filtra apenas pilotos com atividade recente (até 2 anos antes da dt_ref)
	eligible_drivers AS (
		
		SELECT DISTINCT 
				dt_ref,
				driverid
		FROM past_sessions

		WHERE ref_year - year <= 2
	
	),

	-- Identifica rodadas distintas até cada data de referência
	distinct_rounds AS (

		SELECT DISTINCT 
				dt_ref,
				year,
				roundnumber
		FROM past_sessions
	
	),

	-- Ordena rodadas da mais recente para a mais antiga
	ranked_rounds AS (
		
		SELECT dt_ref, 
			   year, 
			   roundnumber,
			   -- Ranking temporal por dt_ref (1 = corrida mais recente)
			   ROW_NUMBER() OVER (PARTITION BY dt_ref ORDER BY year DESC, roundnumber DESC) AS rn
		FROM distinct_rounds
	
	),

	-- Seleciona apenas as n últimas rodadas para cada data de referência
	last_rounds AS (

		SELECT dt_ref,
			   year,
			   roundnumber
		FROM ranked_rounds
		
		-- Quantidade parametrizável das últimas corridas 
		WHERE rn <= {last_races}

	),

	-- Combina histórico passado, pilotos elegíveis e últimas corridas para criação da base final
	tb_results AS (
		SELECT t1.*
		FROM past_sessions AS t1

		INNER JOIN eligible_drivers AS t2
		ON t1.dt_ref = t2.dt_ref 
		AND t1.driverid = t2.driverid
		
		INNER JOIN last_rounds AS t3
		ON t1.dt_ref = t3.dt_ref 
		AND t1.year = t3.year 
		AND t1.roundnumber = t3.roundnumber
	),

	-- Criação das features agregadas por piloto e data de referência
	tb_stats AS (
 
		SELECT
			dt_ref,
			driverid,

			-- Volume de temporadas e corridas
			count(DISTINCT YEAR) AS qtd_seasons,
			count(*) AS qtd_sessions,

			-- Participação por tipo de sessão
			sum(CASE WHEN mode = 'Race' THEN 1 ELSE 0 END) AS qtd_race,
			sum(CASE WHEN mode = 'Sprint' THEN 1 ELSE 0 END) AS qtd_sprint,

			-- Quantidade de corridas terminadas
			sum(CASE WHEN (status = 'Finished' OR status LIKE '+%') THEN 1 ELSE 0 END) AS qtde_sessions_finished,
			sum(CASE WHEN mode = 'Race' AND (status = 'Finished' OR status LIKE '+%') THEN 1 ELSE 0 END) AS qtde_sessions_finished_race,
			sum(CASE WHEN mode = 'Sprint' AND (status = 'Finished' OR status LIKE '+%') THEN 1 ELSE 0 END) AS qtde_sessions_finished_sprint,
			
			-- Performance (corridas vencidas)
			sum(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS qtde_1Pos,
			sum(CASE WHEN position = 1 AND MODE = 'Race' THEN 1 ELSE 0 END) AS qtde_1Pos_race,
			sum(CASE WHEN position = 1 AND MODE = 'Sprint' THEN 1 ELSE 0 END) AS qtde_1Pos_sprint,
			
			-- Consistência (corridas finalizadas no pódio)
			sum(CASE WHEN position <= 3 THEN 1 ELSE 0 END) AS qtde_podios,
			sum(CASE WHEN position <= 3 AND mode = 'Race' THEN 1 ELSE 0 END) AS qtde_podios_race,
			sum(CASE WHEN position <= 3 AND mode = 'Sprint' THEN 1 ELSE 0 END) AS qtde_podios_sprint,
			
			-- Consistência (corridas no top 5)
			sum(CASE WHEN position <= 5 THEN 1 ELSE 0 END) AS qtde_pos5,
			sum(CASE WHEN position <= 5 AND mode = 'Race' THEN 1 ELSE 0 END) AS qtde_pos5_race,
			sum(CASE WHEN position <= 5 AND mode = 'Sprint' THEN 1 ELSE 0 END) AS qtde_pos5_sprint,
			
			-- Qualificação entre os 5 primeiros
			sum(CASE WHEN gridposition <= 5 THEN 1 ELSE 0 END) AS qtde_gridpos5,
			sum(CASE WHEN gridposition <= 5 AND mode = 'Race' THEN 1 ELSE 0 END) AS qtde_gridpos5_race,
			sum(CASE WHEN gridposition <= 5 AND mode = 'Sprint' THEN 1 ELSE 0 END) AS qtde_gridpos5_sprint,
			
			-- Pontuação no total e em cada tipo de sessão
			sum(points) AS qtde_points,
			sum(CASE WHEN mode = 'Race' THEN points END) AS qtde_points_race,
			sum(CASE WHEN mode = 'Sprint' THEN points END) AS qtde_points_sprint,
			
			-- Média da posição de largada na corrida
			avg(gridposition) AS avg_gridposition,
			avg(CASE WHEN mode = 'Race' THEN gridposition END) AS avg_gridposition_race,
			avg(CASE WHEN mode = 'Sprint' THEN gridposition END) AS avg_gridposition_sprint,
			
			-- Média da posição final na corrida
			avg(POSITION) AS avg_position,
			avg(CASE WHEN mode = 'Race' THEN position END) AS avg_position_race,
			avg(CASE WHEN mode = 'Sprint' THEN position END) AS avg_position_sprint,

			-- Quantidade de vezes que largou em primeiro lugar
			sum(CASE WHEN gridposition = 1 THEN 1 ELSE 0 END) AS qtde_1_gridposition,
			sum(CASE WHEN gridposition = 1 AND mode = 'Race' THEN 1 ELSE 0 END) AS qtde_1_gridposition_race,
			sum(CASE WHEN gridposition = 1 AND mode = 'Sprint' THEN 1 ELSE 0 END) AS qtde_1_gridposition_sprint,
			
			-- Quantidade de vezes que largou em primeiro lugar e venceu a corrida
			sum(CASE WHEN gridposition = 1 AND position = 1 THEN 1 ELSE 0 END) AS qtde_pole_win,
			sum(CASE WHEN gridposition = 1 AND position = 1 AND mode = 'Race' THEN 1 ELSE 0 END) AS qtde_pole_win_race,
			sum(CASE WHEN gridposition = 1 AND position = 1 AND mode = 'Sprint' THEN 1 ELSE 0 END) AS qtde_pole_win_sprint,
			
			-- Quantidade de corridas que pontuou
			sum(CASE WHEN points > 0 THEN 1 ELSE 0 END) AS qtd_sessions_with_points,
			sum(CASE WHEN mode = 'Race' AND points > 0 THEN 1 ELSE 0 END) AS qtd_sessions_with_points_race,
			sum(CASE WHEN mode = 'Sprint' AND points > 0 THEN 1 ELSE 0 END) AS qtd_sessions_with_points_sprint,
			
			-- Quantidade das sessões em que a posição de largada é maior que a posição final na corrida
			sum(CASE WHEN position < gridposition THEN 1 ELSE 0 END) AS qtde_sessions_with_overtake,
			sum(CASE WHEN mode = 'Race' AND position < gridposition THEN 1 ELSE 0 END) AS qtde_sessions_with_overtake_race,
			sum(CASE WHEN mode = 'Sprint' AND position < gridposition THEN 1 ELSE 0 END) AS qtde_sessions_with_overtake_sprint,
			
			-- Média de posições ganhas ou perdidas considerando a posição de largada e a posição final
			avg(gridposition - position) AS avg_overtake,
			avg(CASE WHEN mode = 'Race' THEN gridposition - position END) AS avg_overtake_race,
			avg(CASE WHEN mode = 'Sprint' THEN gridposition - position END) AS avg_overtake_sprint
		
		FROM tb_results
		
		GROUP BY dt_ref, driverid

	)

-- Features por piloto ao longo do tempo
SELECT *
SELECT *
FROM tb_stats
ORDER BY dt_ref desc, driverid