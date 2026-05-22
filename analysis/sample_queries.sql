-- =============================================================================
-- Estas queries se pueden ejecutar directamente contra el PostgreSQL
-- del data warehouse (cr_db) una vez que el pipeline haya corrido.
-- Conexión local: psql -h localhost -U cr_user -d cr_db
-- =============================================================================

-- =============================================================================
-- SECCIÓN 1: Análisis de Meta (win rate por carta)
-- =============================================================================

-- Top 20 cartas por win rate (solo cartas con más de 5 apariciones)
-- ¿Qué cartas del meta tienen mejor win rate?
SELECT
    c.card_name,
    c.card_elixir_cost,
    c.card_rarity,
    COUNT(*)                              AS total_games,
    SUM(b.is_victory)                     AS total_wins,
    ROUND(AVG(b.is_victory) * 100, 1)    AS win_rate_pct,
    ROUND(AVG(u.card_level), 1)          AS avg_card_level
FROM fct_cards_usage u
JOIN fct_battles  b ON u.battle_id = b.battle_id
                    AND u.player_tag = b.player_tag
JOIN dim_cards    c ON u.card_id    = c.card_id
WHERE u.played_by = 'Player'
GROUP BY c.card_name, c.card_elixir_cost, c.card_rarity
HAVING COUNT(*) > 5
ORDER BY win_rate_pct DESC
LIMIT 20;


-- Frecuencia de uso de cartas (popularidad en el meta)
-- ¿Cuáles son las cartas más jugadas independientemente del win rate?
SELECT
    c.card_name,
    c.card_rarity,
    COUNT(*)                                          AS total_appearances,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS usage_pct
FROM fct_cards_usage u
JOIN dim_cards c ON u.card_id = c.card_id
GROUP BY c.card_name, c.card_rarity
ORDER BY total_appearances DESC
LIMIT 20;


-- =============================================================================
-- SECCIÓN 2: Análisis de Jugadores
-- =============================================================================

-- Top jugadores del día por trofeos con métricas de actividad
-- ¿Quiénes son los jugadores más activos y exitosos hoy?
SELECT
    p.player_name,
    p.player_trophies,
    f.daily_wins,
    f.daily_battles_played,
    f.daily_trophy_change,
    CASE
        WHEN f.daily_battles_played > 0
        THEN ROUND(f.daily_wins * 100.0 / f.daily_battles_played, 1)
        ELSE NULL
    END AS daily_win_rate_pct
FROM fct_player_daily_stats f
JOIN dim_players p ON f.player_tag = p.player_tag
WHERE f.snapshot_date = CURRENT_DATE
ORDER BY f.total_trophies DESC
LIMIT 20;


-- Evolución de trofeos de un jugador a lo largo del tiempo
-- ¿Cómo ha progresado un jugador específico?
SELECT
    f.snapshot_date,
    f.total_trophies,
    f.daily_trophy_change,
    f.daily_wins,
    f.daily_battles_played
FROM fct_player_daily_stats f
JOIN dim_players p ON f.player_tag = p.player_tag
WHERE p.player_name = 'NOMBRE_DEL_JUGADOR'  -- reemplazar con nombre real
ORDER BY f.snapshot_date;


-- =============================================================================
-- SECCIÓN 3: Análisis de Clanes
-- =============================================================================

-- Clanes más activos por donaciones diarias estimadas
SELECT
    cl.clan_name,
    cl.clan_members,
    SUM(f.estimated_daily_donations) AS total_clan_donations_today
FROM fct_player_daily_stats f
JOIN dim_players p  ON f.player_tag = p.player_tag
JOIN dim_clans   cl ON p.clan_tag   = cl.clan_tag
WHERE f.snapshot_date = CURRENT_DATE
GROUP BY cl.clan_name, cl.clan_members
ORDER BY total_clan_donations_today DESC
LIMIT 10;


-- =============================================================================
-- SECCIÓN 4: Análisis de Batallas
-- =============================================================================

-- Distribución de resultado de batallas por modo de juego
SELECT
    gm.game_mode_name,
    COUNT(*)                           AS total_battles,
    SUM(b.is_victory)                  AS total_wins,
    ROUND(AVG(b.is_victory) * 100, 1) AS win_rate_pct,
    ROUND(AVG(b.player_elixir_leaked), 2) AS avg_elixir_leaked
FROM fct_battles b
JOIN dim_game_modes gm ON b.game_mode_id = gm.game_mode_id
GROUP BY gm.game_mode_name
ORDER BY total_battles DESC;


-- Batallas con 3 coronas (three-crown wins) por fecha
-- ¿Qué porcentaje de las batallas terminan en 3-corona?
SELECT
    snapshot_date,
    COUNT(*)                                 AS total_battles,
    SUM(is_three_crown_win)                  AS three_crown_wins,
    ROUND(AVG(is_three_crown_win) * 100, 1) AS three_crown_rate_pct
FROM fct_battles
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;
