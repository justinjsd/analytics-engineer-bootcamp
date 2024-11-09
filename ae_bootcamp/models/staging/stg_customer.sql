WITH source AS 
    (
        SELECT 
            *
        FROM
            {{ source('stg_northwind', 'customer') }}
    )

SELECT 
    *
FROM
    source