WITH source AS 
    (
        SELECT 
            p.id                 AS product_id
            , p.product_code
            , p.product_name
            , p.description
            , s.company          AS supplier_company
            , p.standard_cost
            , p.list_price
            , p.reorder_level
            , p.target_level
            , p.quantity_per_unit
            , p.discontinued
            , p.minimum_reorder_quantity
            , p.category
            , p.attachments
            , CURRENT_TIMESTAMP() AS insertion_timestamp
        FROM
            {{ ref('stg_products') }} p
        LEFT JOIN
            {{ ref('stg_suppliers') }} s
        ON 
            p.supplier_id = s.id
    )

-- Deduplication of Records based on Product ID - Getting a row number for each product_id
, unique_source AS 
    (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY product_id) AS row_number
        FROM
            source
    )

-- Deduplication of Records based on Product ID - only filtering for ditinct records
SELECT 
    * EXCEPT(row_number)
FROM 
    unique_source
WHERE 
    row_number = 1