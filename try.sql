SELECT 
    c.card_name,
    c.card_elixir_cost,
    COUNT(*) as total_games,
    ROUND(AVG(b.is_victory) * 100, 1) as win_rate_pct
FROM fct_cards_usage u
JOIN fct_battles b ON u.battle_id = b.battle_id
JOIN dim_cards c ON u.card_id = c.card_id
WHERE u.played_by = 'Player'
GROUP BY 1, 2
HAVING COUNT(*) > 5
ORDER BY win_rate_pct DESC;

SELECT 
    p.player_name,
    f.total_trophies,
    f.daily_wins,
    f.daily_trophy_change
FROM fct_player_daily_stats f
JOIN dim_players p ON f.player_tag = p.player_tag
WHERE f.snapshot_date = CURRENT_DATE
ORDER BY f.total_trophies DESC;