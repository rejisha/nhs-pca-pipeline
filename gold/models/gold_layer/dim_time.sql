WITH source AS (
    SELECT DISTINCT
        year_month,
        prescription_month,
        financial_year
    FROM {{ source('silver', 'prescriptions_clean') }}
    WHERE year_month IS NOT NULL
),

final AS (
    SELECT
        CONVERT(VARCHAR(32), HASHBYTES('MD5',
            CAST(year_month AS NVARCHAR(255))), 2)          AS period_id,
        year_month                                           AS period,

        -- Extract year: 202301 → 2023
        CAST(LEFT(CAST(year_month AS VARCHAR), 4) AS INT)   AS year,

        -- Extract month: 202301 → 1
        CAST(RIGHT(CAST(year_month AS VARCHAR), 2) AS INT)  AS month,

        prescription_month,
        financial_year

    FROM source
)

SELECT * FROM final