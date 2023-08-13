--meteorological data have been added to original tables,24 features (min temp/month, relative humidity/month,
--precipitation/month, soil temp/month and sunsine duration/month 
--join all tables to a new table

DROP TABLE DBO.rp18_ndvi_training_data

SELECT * 
INTO all_ndvi_training_data
FROM dbo.bb16_ndvi_training_data
UNION ALL
SELECT * FROM dbo.bb17_ndvi_training_data
UNION ALL
SELECT * FROM dbo.bb18_ndvi_training_data
UNION ALL
SELECT * FROM dbo.ni16_ndvi_training_data
UNION ALL
SELECT * FROM dbo.ni17_ndvi_training_data
UNION ALL
SELECT * FROM dbo.ni18_ndvi_training_data
UNION ALL
SELECT * FROM dbo.nw16_ndvi_training_data
UNION ALL
SELECT * FROM dbo.nw17_ndvi_training_data
UNION ALL
SELECT * FROM dbo.nw18_ndvi_training_data
UNION ALL
SELECT * FROM dbo.nw19_ndvi_training_data
UNION ALL
SELECT * FROM dbo.rp18_ndvi_training_data

--
SELECT * FROM dbo.all_ndvi_training_data
-- features engineer 





(SELECT *, ndvi_min_182_365 / NULLIF(ndvi_max_182_365,0) AS ndvi_min_max_ratio_182_365,
		((precipitation_july + precipitation_aug + precipitation_sep + precipitation_oct + precipitation_nov + precipitation_dec) / 183) as relative_precipitation_182_365,
		((precipitation_jan + precipitation_feb + precipitation_march) / 90) as relative_precipitation_01_90,
		ndvi_trend_196_273 + ndvi_trend_274_332 AS ndvi_trend_sum_196_332,
		ndvi_sum_278_348 + ndvi_sum_001_046 AS ndvi_sum_278_046,
		RANK() OVER(ORDER BY ndvi_min_182_365 DESC) AS ndvi_min_182_365_rank,
	    RANK() OVER(ORDER BY ndvi_mean_182_365 DESC) AS ndvi_mean_182_365_rank, 
		RANK() OVER(ORDER BY ndvi_max_182_365 DESC) AS ndvi_max_182_365_rank,
		(min_temp_july + max_temp_july) / 2 AS mean_temp_july,
		(min_temp_aug + max_temp_aug) / 2 AS mean_temp_aug,
		(min_temp_sep + max_temp_sep) / 2 AS mean_temp_sep,
		(min_temp_oct + max_temp_oct) / 2 AS mean_temp_oct,
		(min_temp_nov + max_temp_nov) / 2 AS mean_temp_nov,
		(min_temp_dec + max_temp_dec) / 2 AS mean_temp_dec,
		(min_temp_jan + max_temp_jan) / 2 AS mean_temp_jan,
		(min_temp_feb + max_temp_feb) / 2 AS mean_temp_feb,
		(min_temp_march + max_temp_march) / 2 AS mean_temp_march,
		(min_temp_july + min_temp_aug + min_temp_sep + min_temp_oct + min_temp_nov + min_temp_dec) / 6 AS min_temp_182_365 ,
		(min_temp_jan + min_temp_feb + min_temp_march) / 3 AS min_temp_01_90,
		(max_temp_july + max_temp_aug + max_temp_sep + max_temp_oct + max_temp_nov + max_temp_dec) / 6 AS max_temp_182_365,
		(max_temp_jan + max_temp_feb + max_temp_march) / 3 AS max_temp_01_90,
		(av_relative_humidity_july + av_relative_humidity_aug + av_relative_humidity_sep + av_relative_humidity_oct + av_relative_humidity_nov + av_relative_humidity_dec) / 6 AS av_relative_humidity_182_365,
		(av_relative_humidity_jan + av_relative_humidity_feb + av_relative_humidity_march) / 3 AS av_relative_humidity_01_90,
		(precipitation_july + precipitation_aug + precipitation_sep + precipitation_oct + precipitation_nov + precipitation_dec) / 6 AS av_precipitation_182_365,
		(precipitation_jan + precipitation_feb + precipitation_march) / 3 AS av_precipitation_01_90,
		(av_soil_temp_july + av_soil_temp_aug + av_soil_temp_sep + av_soil_temp_oct + av_soil_temp_nov + av_soil_temp_dec) / 6 AS av_soil_temp_182_365,
		(av_soil_temp_jan + av_soil_temp_feb + av_soil_temp_march) / 3 AS av_soil_temp_01_90,
		(av_soil_temp_july + av_soil_temp_aug + av_soil_temp_sep + av_soil_temp_oct + av_soil_temp_nov + av_soil_temp_dec) AS av_soil_temp_sum_182_365,
		(av_soil_temp_jan + av_soil_temp_feb + av_soil_temp_march) AS av_soil_temp_sum_01_90,
		(sunshine_duration_july + sunshine_duration_aug + sunshine_duration_sep + sunshine_duration_oct + sunshine_duration_nov + sunshine_duration_dec) AS sunshine_duration_sum_182_365,
		(sunshine_duration_jan + sunshine_duration_feb + sunshine_duration_march) AS sunshine_duration_sum_01_90,
		(sunshine_duration_july + sunshine_duration_aug + sunshine_duration_sep + sunshine_duration_oct + sunshine_duration_nov + sunshine_duration_dec) /6 AS av_sunshine_duration_182_365,
		(sunshine_duration_jan + sunshine_duration_feb + sunshine_duration_march) /3 AS av_sunshine_duration_01_90,
		((min_temp_july + min_temp_aug + min_temp_sep + min_temp_oct + min_temp_nov + min_temp_dec) / (6)) / ((max_temp_july + max_temp_aug + max_temp_sep + max_temp_oct + max_temp_nov + max_temp_dec) / (6)) as min_max_temp_ratio_182_365,
		(((min_temp_july + max_temp_july) / 2) + ((min_temp_aug + max_temp_aug) / 2) + ((min_temp_sep + max_temp_sep) / 2) + ((min_temp_oct + max_temp_oct) / 2) + ((min_temp_nov + max_temp_nov) / 2) + ((min_temp_dec + max_temp_dec) / 2 )) /6 as mean_temp_182_365,
		(((min_temp_jan + max_temp_jan) / 2) + ((min_temp_feb + max_temp_feb) / 2) + ((min_temp_march + max_temp_march) / 2)) / 3 as mean_temp_01_90,
		((((min_temp_july + max_temp_july) / 2) + ((min_temp_aug + max_temp_aug) / 2) + ((min_temp_sep + max_temp_sep) / 2) + ((min_temp_oct + max_temp_oct) / 2) + ((min_temp_nov + max_temp_nov) / 2) + ((min_temp_dec + max_temp_dec) / 2 )) /6) / ((precipitation_july + precipitation_aug + precipitation_sep + precipitation_oct + precipitation_nov + precipitation_dec) / 6) as mean_temp_to_av_precipitation_ratio_182_365,
		(((min_temp_jan + max_temp_jan) / 2) + ((min_temp_feb + max_temp_feb) / 2) + ((min_temp_march + max_temp_march) / 2) / 3) / ((precipitation_jan + precipitation_feb + precipitation_march) / 3) as mean_temp_to_av_precipitation_ratio_01_90,
		((av_soil_temp_july + av_soil_temp_aug + av_soil_temp_sep + av_soil_temp_oct + av_soil_temp_nov + av_soil_temp_dec) / 6) / ((precipitation_july + precipitation_aug + precipitation_sep + precipitation_oct + precipitation_nov + precipitation_dec) /6) as av_soil_temp_to_av_precipitation_ratio_182_365,
		((av_soil_temp_jan + av_soil_temp_feb + av_soil_temp_march) / 3) / ((precipitation_jan + precipitation_feb + precipitation_march) / 3) as av_soil_temp_to_av_precipitation_ratio_01_90,
		((sunshine_duration_july + sunshine_duration_aug + sunshine_duration_sep + sunshine_duration_oct + sunshine_duration_nov + sunshine_duration_dec) /6) / ((precipitation_july + precipitation_aug + precipitation_sep + precipitation_oct + precipitation_nov + precipitation_dec) / 6) as sunshine_dur_to_av_precipitation_ratio_182_365,
		((sunshine_duration_jan + sunshine_duration_feb + sunshine_duration_march) /3) / ((precipitation_jan + precipitation_feb + precipitation_march) / 3) as sunshine_dur_to_av_precipitation_ratio_01_90
	FROM dbo.all_ndvi_training_data)

	
	 

 




