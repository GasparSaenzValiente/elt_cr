with player_stats as (
    select * from {{ ref('stg_players') }}
),
clan_activity as (
    select * from {{ ref('stg_clan_members') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['p.player_tag', 'p.snapshot_date']) }} as daily_stats_key,
    p.player_tag,
    p.clan_tag,
    p.snapshot_date,
        
    p.player_trophies as total_trophies,
    p.player_wins as total_wins,

    c.member_donations as week_total_donations,
    c.member_donations_received as week_total_donations_received,
    
    -- daily donations
    case 
        when coalesce(c.member_donations, 0) < lag(coalesce(c.member_donations, 0), 1, 0) over (partition by p.player_tag order by p.snapshot_date)
        -- weekly donations restarted
        then coalesce(c.member_donations, 0)
        -- weekly report
        else coalesce(c.member_donations, 0) - lag(coalesce(c.member_donations, 0), 1, 0) over (partition by p.player_tag order by p.snapshot_date)
    end as estimated_daily_donations,

    -- trophy change
    p.player_trophies - lag(p.player_trophies, 1, p.player_trophies) over (
        partition by p.player_tag 
        order by p.snapshot_date
    ) as daily_trophy_change,
        
    -- daily wins
    p.player_wins - lag(p.player_wins, 1, p.player_wins) over (
        partition by p.player_tag 
        order by p.snapshot_date
    ) as daily_wins,
        
    -- daily battles
    p.player_battle_count - lag(p.player_battle_count, 1, p.player_battle_count) over (
        partition by p.player_tag 
        order by p.snapshot_date
    ) as daily_battles_played



from player_stats p 
left join clan_activity c
on p.player_tag = c.member_tag 
and p.snapshot_date = c.snapshot_date