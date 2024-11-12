WITH source AS 
    (
        SELECT
            id                  AS employee_id 
            , company
            , last_name
            , first_name
            , email_address
            , job_title
            , business_phone
            , home_phone
            , mobile_phone
            , fax_number
            , address
            , city
            , state_province
            , zip_postal_code
            , country_region
            , web_page
            , notes
            , attachments
            , CURRENT_TIMESTAMP() AS insertion_timestamp
        FROM
            {{ ref('stg_employees') }}
    )

-- Deduplication of Records based on Employee ID - Getting a row number for each employee_id
, unique_source AS 
    (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY employee_id) AS row_number
        FROM
            source
    )

-- Deduplication of Records based on Employee ID - only filtering for ditinct records
SELECT 
    * EXCEPT(row_number)
FROM 
    unique_source
WHERE 
    row_number = 1