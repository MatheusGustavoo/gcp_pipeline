{{
    config(
        alias='transformed_data',
        materialized='table',
        tags=['daily', 'financial']
    )
}}

WITH source_data AS (
    SELECT
        ID1,
        ID2,
        ID3,
        String1,
        String2,
        String3,
        Float1,
        Float2,
        Float3,
        Status
    FROM {{ source('data_raw', 'raw_data') }}
),

cleaned AS (
    SELECT
        -- IDs
        TRIM(ID1) AS transaction_id,
        TRIM(ID2) AS merchant_id,
        TRIM(ID3) AS customer_id,
        
        -- Descriptive fields
        UPPER(TRIM(String1)) AS business_category,
        UPPER(TRIM(String2)) AS currency_code,
        TRIM(String3) AS reference_token,
        
        -- Financial amounts (already NUMERIC from source)
        Float1 AS gross_amount,
        Float2 AS fee_amount,
        Float3 AS net_amount,
        
        -- Status
        TRIM(Status) AS transaction_status
        
    FROM source_data
),

transformed AS (
    SELECT
        *,
        
        -- Calculate tax (10% of gross amount)
        ROUND(gross_amount * 0.10, 2) AS tax_amount,
        
        -- Calculate expected net (for validation)
        ROUND(gross_amount - fee_amount, 2) AS calculated_net,
        
        -- Derive final status based on transaction_status
        CASE
            WHEN transaction_status = 'Settled' THEN 'Completed'
            WHEN transaction_status = 'Failed' THEN 'Failed'
            WHEN transaction_status = 'Reversed' THEN 'Reversed'
            WHEN transaction_status = 'Cancelled' THEN 'Cancelled'
            WHEN transaction_status IN ('Pending', 'In Review') THEN 'Pending'
            ELSE 'Unknown'
        END AS final_status,
        
        -- Fee percentage for analysis
        CASE 
            WHEN gross_amount > 0 THEN ROUND((fee_amount / gross_amount) * 100, 2)
            ELSE 0
        END AS fee_percentage,
        
        -- Currency region grouping
        CASE
            WHEN currency_code IN ('EUR', 'GBP') THEN 'Europe'
            WHEN currency_code IN ('USD') THEN 'North America'
            WHEN currency_code IN ('PLN', 'SEK', 'NOK') THEN 'Nordic/Eastern Europe'
            ELSE 'Other'
        END AS currency_region,
        
        -- Business category grouping
        CASE
            WHEN business_category IN ('RETAIL', 'WHOLESALE', 'MARKETPLACE') THEN 'Physical Commerce'
            WHEN business_category IN ('ONLINE', 'SUBSCRIPTION') THEN 'Digital Commerce'
            WHEN business_category = 'B2B' THEN 'Business Services'
            ELSE 'Other'
        END AS commerce_type,
        
        -- Data quality flags
        CASE
            WHEN ABS(net_amount - (gross_amount - fee_amount)) > 0.01 THEN TRUE
            ELSE FALSE
        END AS net_amount_mismatch_flag,
        
        CASE
            WHEN fee_amount > gross_amount THEN TRUE
            ELSE FALSE
        END AS fee_exceeds_amount_flag,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_loaded_at
        
    FROM cleaned
)

SELECT * FROM transformed