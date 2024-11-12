SELECT
  FORMAT_DATE('%F', dates) as id,
  dates AS full_date,
  EXTRACT(YEAR FROM dates) AS year,
  EXTRACT(WEEK FROM dates) AS year_week,
  EXTRACT(DAY FROM dates) AS year_day,
  EXTRACT(YEAR FROM dates) AS fiscal_year,
  FORMAT_DATE('%Q', dates) as fiscal_qtr,
  EXTRACT(MONTH FROM dates) AS month,
  FORMAT_DATE('%B', dates) as month_name,
  FORMAT_DATE('%w', dates) AS week_day,
  FORMAT_DATE('%A', dates) AS day_name,
  CASE WHEN FORMAT_DATE('%A', dates) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END AS day_is_weekday,
FROM 
    (
        SELECT
            *
        FROM
            UNNEST(GENERATE_DATE_ARRAY('2014-01-01', '2050-01-01', INTERVAL 1 DAY)) AS dates
    )