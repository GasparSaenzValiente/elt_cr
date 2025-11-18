with source_data as (
    select * from {{ source('cr_db_landing', 'landing_players') }}
)
select 
    cast(tag as varchar(10)) as player_tag,
    cast(clan_tag as varchar(10)) as clan_tag,
    fav_card_id as player_fav_card_id,

    cast(name as varchar(15)) as player_name,
    cast(exp_level as smallint) as player_exp_level,
    cast(trophies as smallint) as player_trophies,

    wins as player_wins,
    losses as player_losses,
    battle_count as player_battle_count,
    three_crown_wins as player_three_crown_wins,
    war_day_wins as player_war_day_wins,
    cast(snapshot_date as date) as snapshot_date
from source_data