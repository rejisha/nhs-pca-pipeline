WITH source AS (
    SELECT DISTINCT
        region_name
    FROM {{ source('silver', 'prescriptions_clean') }}
    WHERE region_name IS NOT NULL
),

final AS (
    SELECT
        CONVERT(VARCHAR(32), HASHBYTES('MD5',
            CAST(region_name AS NVARCHAR(255))), 2)  AS region_id,
        region_name
    FROM source
)

SELECT * FROM final