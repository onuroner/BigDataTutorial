with t as(
  select start_station_id, infos.short_name, end_station_id, infos_2.short_name,AVG(CASE WHEN member_gender = 'Male' THEN duration_sec END) AS male_avg_duration,
  AVG(CASE WHEN member_gender = 'Female' THEN duration_sec END) AS female_avg_duration
  from  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` trips 
  inner join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` infos on cast(trips.start_station_id as STRING) = infos.station_id   inner join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` infos_2 on cast(trips.end_station_id as STRING) = infos_2.station_id where trips.member_gender = 'Male' or member_gender = 'Female'
  group by start_station_id, infos.short_name, end_station_id, infos_2.short_name 
  HAVING 
  male_avg_duration IS NOT NULL
  AND female_avg_duration IS NOT NULL
)

select * from t