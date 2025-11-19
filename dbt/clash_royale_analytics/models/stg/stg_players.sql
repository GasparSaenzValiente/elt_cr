with source_data as (
    select * from {{ source('cr_db_landing', 'landing_players') }}
)
select distinct on (tag, snapshot_date)
    cast(tag as varchar(10)) as player_tag,
    cast(clan::json->>'tag' AS varchar(15)) AS clan_tag,
    cast("currentFavouriteCard"::json->>'id' AS varchar(50)) AS fav_card_id,

    cast(name as varchar(15)) as player_name,
    cast("expLevel" as smallint) as player_exp_level,
    cast(trophies as smallint) as player_trophies,

    wins as player_wins,
    losses as player_losses,
    cast("battleCount" as int) as player_battle_count,
    cast("threeCrownWins" as int) as player_three_crown_wins,
    cast("warDayWins" as smallint) as player_war_day_wins,

    cast(snapshot_date as date) as snapshot_date
from source_data
order by tag, snapshot_date