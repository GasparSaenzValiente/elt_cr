-- 23:54:11  4 of 29 FAIL 1 accepted_values_fct_battles_player_crowns__0__1__2__3 ........... [FAIL 1 in 0.18s]
-- 23:54:11  2 of 29 ERROR accepted_values_dim_cards_rarity__Common__Rare__Epic__Legendary__Champion  [ERROR in 0.20s]

SELECT * FROM fct_battles
WHERE player_crowns not in (0,1,2,3)
OR player_crowns IS NULL;

SELECT * FROM dim_cards
WHERE card_rarity not in ('common','rare','epic','legendary','champion')