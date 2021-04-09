Using query data that has been categorized/labelled from GSC (found in the `WITH()` statement below), we're able to look at % change overtime with BigQuery + 
the `LAG()` function; partitioning it by the label/category we can see a rolled up performance summary of a 
particular query corpus. 

```
WITH gsc_page_data as (
 SELECT
   PARSE_DATE ("%Y-%m-%d", CAST(DATE(month_pulled) AS STRING)) as date,
   category,
   sum(clicks) as Clicks,
   sum(impressions) as Impressions,
   avg(ctr) as CTR
 FROM `lsg-machine-learning.{dataset}.classified_gsc_data`
 GROUP BY category, date
 ORDER BY date asc
)
SELECT
 date,
 category,
 Clicks,
 SAFE_DIVIDE(
     Clicks - LAG(Clicks, 1) OVER (PARTITION BY category ORDER BY date ASC),
     LAG(Clicks, 1) OVER (PARTITION BY category ORDER BY date ASC)
     ) as MoM_clicks_perc_change,

 SAFE_DIVIDE(
     Clicks - LAG(Clicks, 12) OVER (PARTITION BY category ORDER BY date ASC),
     LAG(Clicks, 12) OVER (PARTITION BY category ORDER BY date ASC)
     ) as YoY_clicks_perc_change,
 Impressions,

 SAFE_DIVIDE (
   Impressions - LAG(Impressions, 1) OVER (PARTITION BY category ORDER BY date ASC),
   LAG(Impressions, 1) OVER (PARTITION BY category ORDER BY date ASC)
   )  as MoM_imp_perc_change,

 SAFE_DIVIDE (
   Impressions - LAG(Impressions, 12) OVER (PARTITION BY category ORDER BY date ASC),
   LAG(Impressions, 12) OVER (PARTITION BY category ORDER BY date ASC)
   ) as YoY_imp_perc_change,

 SAFE_DIVIDE (
   CTR - LAG(CTR, 1) OVER (PARTITION BY category ORDER BY date ASC),
   LAG(CTR, 1) OVER (PARTITION BY category ORDER BY date ASC)
   ) as MoM_ctr_perc_change,

 SAFE_DIVIDE (
   CTR - LAG(CTR, 12) OVER (PARTITION BY category ORDER BY date ASC),
   LAG(CTR, 12) OVER (PARTITION BY category ORDER BY date ASC)
   ) as YoY_ctr_perc_change

FROM gsc_page_data
order by category, date
```


Because there may be some months/years that have no clicks, we wrap our lag function in a `SAFE_DIVIDE()` to prevent
any errors from division by 0. 

