with players as (
    select
        *,
        row_number() over (
            partition by player_tag
            order by snapshot_date desc
        ) as rn
    from "cr_db"."public"."stg_players"
)
select
    player_tag,
    clan_tag,
    player_name,
    player_exp_level,
    player_trophies,
    player_wins,
    player_losses,
    player_battle_count,
    player_three_crown_wins,
    player_war_day_wins,
    fav_card_id
from players
where rn = 1