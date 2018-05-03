/*secondary pitch usage change*/
WITH total_pitches AS
(
  SELECT player_name,
         SUM(CASE WHEN game_year = 2017 THEN 1 ELSE 0 END) AS total_pitches_2017,
         SUM(CASE WHEN game_year = 2018 THEN 1 ELSE 0 END) AS total_pitches_2018
  FROM statcast.pitch_events
  WHERE game_year IN (2017,2018)
  AND pitch_type<>''
  GROUP BY player_name
)
SELECT statcast.pitch_events.player_name,
       CASE WHEN statcast.pitch_events.pitch_type IN ('SI','FT') THEN 'FT' ELSE statcast.pitch_events.pitch_type END AS pitch_types,
       total_pitches_2017,
       total_pitches_2018,
       SUM(CASE WHEN game_year = 2017 THEN 1 ELSE 0 END)::DECIMAL / total_pitches_2017 AS percent_thrown_2017,
       SUM(CASE WHEN game_year = 2018 THEN 1 ELSE 0 END)::DECIMAL / total_pitches_2018 AS percent_thrown_2018,
       (SUM(CASE WHEN game_year = 2018 THEN 1 ELSE 0 END)::DECIMAL / total_pitches_2018) - (SUM(CASE WHEN game_year = 2017 THEN 1 ELSE 0 END)::DECIMAL / total_pitches_2017) AS throw_rate_delta
FROM statcast.pitch_events
  RIGHT JOIN total_pitches ON total_pitches.player_name = statcast.pitch_events.player_name
WHERE statcast.pitch_events.game_year IN (2017,2018)
AND   total_pitches_2017 > 1000
AND   total_pitches_2018 > 250
AND   CASE WHEN statcast.pitch_events.pitch_type IN ('SI','FT') THEN 'FT' ELSE statcast.pitch_events.pitch_type END NOT IN ('','FT','FF')
GROUP BY statcast.pitch_events.player_name,
         CASE WHEN statcast.pitch_events.pitch_type IN ('SI','FT') THEN 'FT' ELSE statcast.pitch_events.pitch_type END,
         total_pitches_2017,
         total_pitches_2018
ORDER BY throw_rate_delta ASC
