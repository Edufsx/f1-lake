/* 
Consolida as feature stores por piloto (driverid) e data de referência (dt_ref),
considerando diferentes janelas temporais:
- Histórico completo (vida);
- Últimas 10 corridas; 
- Últimas 20 corridas;
- Últimas 40 corridas.
*/

SELECT tb_life.dt_ref || '_' || tb_life.driverid AS id,

       tb_life.dt_ref,
       tb_life.driverid,

       -- ==========================
       -- FEATURES HISTÓRICAS (VIDA)
       -- ==========================
       tb_life.qtde_gridpos5_sprint AS qtde_gridpos5_sprint_life,
       tb_life.qtd_sessions_with_points_sprint AS qtd_sessions_with_points_sprint_life,
       tb_life.qtde_pos5_sprint AS qtde_pos5_sprint_life,
       tb_life.qtde_gridpos5 AS qtde_gridpos5_life,
       tb_life.qtde_gridpos5_race AS qtde_gridpos5_race_life,
       tb_life.qtd_sessions_with_points_race AS qtd_sessions_with_points_race_life,
       tb_life.qtd_seasons AS qtd_seasons_life,
       tb_life.qtd_sessions AS qtd_sessions_life,
       tb_life.qtde_sessions_finished AS qtde_sessions_finished_life,
       tb_life.qtd_race AS qtd_race_life,
       tb_life.qtde_sessions_finished_race AS qtde_sessions_finished_race_life,
       tb_life.qtd_sprint AS qtd_sprint_life,
       tb_life.qtde_sessions_finished_sprint AS qtde_sessions_finished_sprint_life,
       tb_life.qtde_pos5 AS qtde_pos5_life,
       tb_life.qtde_pos5_race AS qtde_pos5_race_life,
       tb_life.qtde_1_gridposition AS qtde_1_gridposition_life,
       tb_life.qtde_1_gridposition_race AS qtde_1_gridposition_race_life,
       tb_life.qtde_1_gridposition_sprint AS qtde_1_gridposition_sprint_life,
       tb_life.qtd_sessions_with_points AS qtd_sessions_with_points_life,
       tb_life.avg_position_race AS avg_position_race_life,
       tb_life.avg_overtake AS avg_overtake_life,
       tb_life.avg_overtake_race AS avg_overtake_race_life,
       tb_life.avg_overtake_sprint AS avg_overtake_sprint_life,
       tb_life.qtde_pole_win AS qtde_pole_win_life,
       tb_life.qtde_pole_win_race AS qtde_pole_win_race_life,
       tb_life.qtde_pole_win_sprint AS qtde_pole_win_sprint_life,
       tb_life.qtde_1pos_race AS qtde_1pos_race_life,
       tb_life.qtde_1pos_sprint AS qtde_1pos_sprint_life,
       tb_life.qtde_podios AS qtde_podios_life,
       tb_life.qtde_podios_race AS qtde_podios_race_life,
       tb_life.avg_gridposition AS avg_gridposition_life,
       tb_life.avg_gridposition_race AS avg_gridposition_race_life,
       tb_life.avg_position AS avg_position_life,
       tb_life.avg_position_sprint AS avg_position_sprint_life,
       tb_life.qtde_sessions_with_overtake AS qtde_sessions_with_overtake_life,
       tb_life.qtde_sessions_with_overtake_race AS qtde_sessions_with_overtake_race_life,
       tb_life.qtde_sessions_with_overtake_sprint AS qtde_sessions_with_overtake_sprint_life,
       tb_life.qtde_points_sprint AS qtde_points_sprint_life,
       tb_life.avg_gridposition_sprint AS avg_gridposition_sprint_life,
       tb_life.qtde_podios_sprint AS qtde_podios_sprint_life,
       tb_life.qtde_points AS qtde_points_life,
       tb_life.qtde_1pos AS qtde_1pos_life,
       tb_life.qtde_points_race AS qtde_points_race_life,
       
       -- ============================
       -- FEATURES ÚLTIMAS 10 CORRIDAS
       -- ============================
       tb_last_10.qtde_gridpos5_sprint AS qtde_gridpos5_sprint_last_10,
       tb_last_10.qtd_sessions_with_points_sprint AS qtd_sessions_with_points_sprint_last_10,
       tb_last_10.qtde_pos5_sprint AS qtde_pos5_sprint_last_10,
       tb_last_10.qtde_gridpos5 AS qtde_gridpos5_last_10,
       tb_last_10.qtde_gridpos5_race AS qtde_gridpos5_race_last_10,
       tb_last_10.qtd_sessions_with_points_race AS qtd_sessions_with_points_race_last_10,
       tb_last_10.qtd_seasons AS qtd_seasons_last_10,
       tb_last_10.qtd_sessions AS qtd_sessions_last_10,
       tb_last_10.qtde_sessions_finished AS qtde_sessions_finished_last_10,
       tb_last_10.qtd_race AS qtd_race_last_10,
       tb_last_10.qtde_sessions_finished_race AS qtde_sessions_finished_race_last_10,
       tb_last_10.qtd_sprint AS qtd_sprint_last_10,
       tb_last_10.qtde_sessions_finished_sprint AS qtde_sessions_finished_sprint_last_10,
       tb_last_10.qtde_pos5 AS qtde_pos5_last_10,
       tb_last_10.qtde_pos5_race AS qtde_pos5_race_last_10,
       tb_last_10.qtde_1_gridposition AS qtde_1_gridposition_last_10,
       tb_last_10.qtde_1_gridposition_race AS qtde_1_gridposition_race_last_10,
       tb_last_10.qtde_1_gridposition_sprint AS qtde_1_gridposition_sprint_last_10,
       tb_last_10.qtd_sessions_with_points AS qtd_sessions_with_points_last_10,
       tb_last_10.avg_position_race AS avg_position_race_last_10,
       tb_last_10.avg_overtake AS avg_overtake_last_10,
       tb_last_10.avg_overtake_race AS avg_overtake_race_last_10,
       tb_last_10.avg_overtake_sprint AS avg_overtake_sprint_last_10,
       tb_last_10.qtde_pole_win AS qtde_pole_win_last_10,
       tb_last_10.qtde_pole_win_race AS qtde_pole_win_race_last_10,
       tb_last_10.qtde_pole_win_sprint AS qtde_pole_win_sprint_last_10,
       tb_last_10.qtde_1pos_race AS qtde_1pos_race_last_10,
       tb_last_10.qtde_1pos_sprint AS qtde_1pos_sprint_last_10,
       tb_last_10.qtde_podios AS qtde_podios_last_10,
       tb_last_10.qtde_podios_race AS qtde_podios_race_last_10,
       tb_last_10.avg_gridposition AS avg_gridposition_last_10,
       tb_last_10.avg_gridposition_race AS avg_gridposition_race_last_10,
       tb_last_10.avg_position AS avg_position_last_10,
       tb_last_10.avg_position_sprint AS avg_position_sprint_last_10,
       tb_last_10.qtde_sessions_with_overtake AS qtde_sessions_with_overtake_last_10,
       tb_last_10.qtde_sessions_with_overtake_race AS qtde_sessions_with_overtake_race_last_10,
       tb_last_10.qtde_sessions_with_overtake_sprint AS qtde_sessions_with_overtake_sprint_last_10,
       tb_last_10.qtde_points_sprint AS qtde_points_sprint_last_10,
       tb_last_10.avg_gridposition_sprint AS avg_gridposition_sprint_last_10,
       tb_last_10.qtde_podios_sprint AS qtde_podios_sprint_last_10,
       tb_last_10.qtde_points AS qtde_points_last_10,
       tb_last_10.qtde_1pos AS qtde_1pos_last_10,
       tb_last_10.qtde_points_race AS qtde_points_race_last_10,

       -- ============================
       -- FEATURES ÚLTIMAS 20 CORRIDAS
       -- ============================
       tb_last_20.qtde_gridpos5_sprint AS qtde_gridpos5_sprint_last_20,
       tb_last_20.qtd_sessions_with_points_sprint AS qtd_sessions_with_points_sprint_last_20,
       tb_last_20.qtde_pos5_sprint AS qtde_pos5_sprint_last_20,
       tb_last_20.qtde_gridpos5 AS qtde_gridpos5_last_20,
       tb_last_20.qtde_gridpos5_race AS qtde_gridpos5_race_last_20,
       tb_last_20.qtd_sessions_with_points_race AS qtd_sessions_with_points_race_last_20,
       tb_last_20.qtd_seasons AS qtd_seasons_last_20,
       tb_last_20.qtd_sessions AS qtd_sessions_last_20,
       tb_last_20.qtde_sessions_finished AS qtde_sessions_finished_last_20,
       tb_last_20.qtd_race AS qtd_race_last_20,
       tb_last_20.qtde_sessions_finished_race AS qtde_sessions_finished_race_last_20,
       tb_last_20.qtd_sprint AS qtd_sprint_last_20,
       tb_last_20.qtde_sessions_finished_sprint AS qtde_sessions_finished_sprint_last_20,
       tb_last_20.qtde_pos5 AS qtde_pos5_last_20,
       tb_last_20.qtde_pos5_race AS qtde_pos5_race_last_20,
       tb_last_20.qtde_1_gridposition AS qtde_1_gridposition_last_20,
       tb_last_20.qtde_1_gridposition_race AS qtde_1_gridposition_race_last_20,
       tb_last_20.qtde_1_gridposition_sprint AS qtde_1_gridposition_sprint_last_20,
       tb_last_20.qtd_sessions_with_points AS qtd_sessions_with_points_last_20,
       tb_last_20.avg_position_race AS avg_position_race_last_20,
       tb_last_20.avg_overtake AS avg_overtake_last_20,
       tb_last_20.avg_overtake_race AS avg_overtake_race_last_20,
       tb_last_20.avg_overtake_sprint AS avg_overtake_sprint_last_20,
       tb_last_20.qtde_pole_win AS qtde_pole_win_last_20,
       tb_last_20.qtde_pole_win_race AS qtde_pole_win_race_last_20,
       tb_last_20.qtde_pole_win_sprint AS qtde_pole_win_sprint_last_20,
       tb_last_20.qtde_1pos_race AS qtde_1pos_race_last_20,
       tb_last_20.qtde_1pos_sprint AS qtde_1pos_sprint_last_20,
       tb_last_20.qtde_podios AS qtde_podios_last_20,
       tb_last_20.qtde_podios_race AS qtde_podios_race_last_20,
       tb_last_20.avg_gridposition AS avg_gridposition_last_20,
       tb_last_20.avg_gridposition_race AS avg_gridposition_race_last_20,
       tb_last_20.avg_position AS avg_position_last_20,
       tb_last_20.avg_position_sprint AS avg_position_sprint_last_20,
       tb_last_20.qtde_sessions_with_overtake AS qtde_sessions_with_overtake_last_20,
       tb_last_20.qtde_sessions_with_overtake_race AS qtde_sessions_with_overtake_race_last_20,
       tb_last_20.qtde_sessions_with_overtake_sprint AS qtde_sessions_with_overtake_sprint_last_20,
       tb_last_20.qtde_points_sprint AS qtde_points_sprint_last_20,
       tb_last_20.avg_gridposition_sprint AS avg_gridposition_sprint_last_20,
       tb_last_20.qtde_podios_sprint AS qtde_podios_sprint_last_20,
       tb_last_20.qtde_points AS qtde_points_last_20,
       tb_last_20.qtde_1pos AS qtde_1pos_last_20,
       tb_last_20.qtde_points_race AS qtde_points_race_last_20,

       -- ============================
       -- FEATURES ÚLTIMAS 40 CORRIDAS
       -- ============================
       tb_last_40.qtde_gridpos5_sprint AS qtde_gridpos5_sprint_last_40,
       tb_last_40.qtd_sessions_with_points_sprint AS qtd_sessions_with_points_sprint_last_40,
       tb_last_40.qtde_pos5_sprint AS qtde_pos5_sprint_last_40,
       tb_last_40.qtde_gridpos5 AS qtde_gridpos5_last_40,
       tb_last_40.qtde_gridpos5_race AS qtde_gridpos5_race_last_40,
       tb_last_40.qtd_sessions_with_points_race AS qtd_sessions_with_points_race_last_40,
       tb_last_40.qtd_seasons AS qtd_seasons_last_40,
       tb_last_40.qtd_sessions AS qtd_sessions_last_40,
       tb_last_40.qtde_sessions_finished AS qtde_sessions_finished_last_40,
       tb_last_40.qtd_race AS qtd_race_last_40,
       tb_last_40.qtde_sessions_finished_race AS qtde_sessions_finished_race_last_40,
       tb_last_40.qtd_sprint AS qtd_sprint_last_40,
       tb_last_40.qtde_sessions_finished_sprint AS qtde_sessions_finished_sprint_last_40,
       tb_last_40.qtde_pos5 AS qtde_pos5_last_40,
       tb_last_40.qtde_pos5_race AS qtde_pos5_race_last_40,
       tb_last_40.qtde_1_gridposition AS qtde_1_gridposition_last_40,
       tb_last_40.qtde_1_gridposition_race AS qtde_1_gridposition_race_last_40,
       tb_last_40.qtde_1_gridposition_sprint AS qtde_1_gridposition_sprint_last_40,
       tb_last_40.qtd_sessions_with_points AS qtd_sessions_with_points_last_40,
       tb_last_40.avg_position_race AS avg_position_race_last_40,
       tb_last_40.avg_overtake AS avg_overtake_last_40,
       tb_last_40.avg_overtake_race AS avg_overtake_race_last_40,
       tb_last_40.avg_overtake_sprint AS avg_overtake_sprint_last_40,
       tb_last_40.qtde_pole_win AS qtde_pole_win_last_40,
       tb_last_40.qtde_pole_win_race AS qtde_pole_win_race_last_40,
       tb_last_40.qtde_pole_win_sprint AS qtde_pole_win_sprint_last_40,
       tb_last_40.qtde_1pos_race AS qtde_1pos_race_last_40,
       tb_last_40.qtde_1pos_sprint AS qtde_1pos_sprint_last_40,
       tb_last_40.qtde_podios AS qtde_podios_last_40,
       tb_last_40.qtde_podios_race AS qtde_podios_race_last_40,
       tb_last_40.avg_gridposition AS avg_gridposition_last_40,
       tb_last_40.avg_gridposition_race AS avg_gridposition_race_last_40,
       tb_last_40.avg_position AS avg_position_last_40,
       tb_last_40.avg_position_sprint AS avg_position_sprint_last_40,
       tb_last_40.qtde_sessions_with_overtake AS qtde_sessions_with_overtake_last_40,
       tb_last_40.qtde_sessions_with_overtake_race AS qtde_sessions_with_overtake_race_last_40,
       tb_last_40.qtde_sessions_with_overtake_sprint AS qtde_sessions_with_overtake_sprint_last_40,
       tb_last_40.qtde_points_sprint AS qtde_points_sprint_last_40,
       tb_last_40.avg_gridposition_sprint AS avg_gridposition_sprint_last_40,
       tb_last_40.qtde_podios_sprint AS qtde_podios_sprint_last_40,
       tb_last_40.qtde_points AS qtde_points_last_40,
       tb_last_40.qtde_1pos AS qtde_1pos_last_40,
       tb_last_40.qtde_points_race AS qtde_points_race_last_40,

       -- =====================
       -- Atributos Descritivos
       -- =====================
       tb_results.fullname,
       tb_results.teamname,
       tb_results.teamcolor

-- Base principal (histórico completo)
FROM `f1_lake_2_silver`.`fs_f1_driver_life` AS tb_life

-- Junta features de curto prazo (últimas 10 corridas)
INNER JOIN `f1_lake_2_silver`.`fs_f1_driver_last_10` AS tb_last_10
ON tb_life.driverid = tb_last_10.driverid
AND tb_life.dt_ref = tb_last_10.dt_ref

-- Junta features de médio prazo (últimas 20 corridas)
INNER JOIN `f1_lake_2_silver`.`fs_f1_driver_last_20` AS tb_last_20
ON tb_life.driverid = tb_last_20.driverid
AND tb_life.dt_ref = tb_last_20.dt_ref

-- Junta features de maior janela (últimas 40 corridas)
INNER JOIN `f1_lake_2_silver`.`fs_f1_driver_last_40` AS tb_last_40
ON tb_life.driverid = tb_last_40.driverid
AND tb_life.dt_ref = tb_last_40.dt_ref

-- Adiciona informações descritivas do piloto/equipe
LEFT JOIN `f1_lake_2_bronze`.`f1_results` AS tb_results
ON tb_life.driverid = tb_results.driverid
AND DATE(tb_life.dt_ref) = DATE(tb_results.date)