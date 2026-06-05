-- Test transformed data AFTER transformation
SELECT 
    reference_token
FROM {{ source('financial_data_dev_source', 'transformed_data') }}
WHERE reference_token IS NULL  -- Returns rows that FAIL the test