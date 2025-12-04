SELECT
    ROW_NUMBER() OVER (ORDER BY acquisition_channel) AS acquisition_channel_key,
    acquisition_channel AS acquisition_channel_name
FROM (
    SELECT DISTINCT acquisition_channel
    FROM stg_customers
) s;
