WITH source AS (
    SELECT
        year_month,
        region_name,
        icb_code,
        icb_name,
        bnf_presentation_code,
        items,
        nic,
        total_quantity,
        prescription_month,
        financial_year
    FROM {{ source('silver', 'prescriptions_clean') }}
    WHERE bnf_presentation_code IS NOT NULL
      AND region_name           IS NOT NULL
      AND year_month            IS NOT NULL
),
with_bnf AS (
    SELECT s.*, d.bnf_id
    FROM source s
    LEFT JOIN {{ ref('dim_bnf') }} d
        ON s.bnf_presentation_code = d.bnf_code
),
with_region AS (
    SELECT w.*, r.region_id
    FROM with_bnf w
    LEFT JOIN {{ ref('dim_region') }} r
        ON w.region_name = r.region_name
),
with_time AS (
    SELECT w.*, t.period_id
    FROM with_region w
    LEFT JOIN {{ ref('dim_time') }} t
        ON w.year_month = t.period
),
final AS (
    SELECT
        CONVERT(VARCHAR(32), HASHBYTES('MD5',
            CAST(
                CONCAT(
                    CAST(year_month AS VARCHAR),
                    '|', bnf_presentation_code,
                    '|', region_name,
                    '|', icb_code
                ) AS NVARCHAR(500)
            )
        ), 2)                   AS prescription_id,
        bnf_id,
        region_id,
        period_id,
        icb_code,
        icb_name,
        prescription_month,
        financial_year,
        SUM(items)              AS items_prescribed,
        SUM(nic)                AS net_ingredient_cost,
        SUM(total_quantity)     AS total_quantity
    FROM with_time
    GROUP BY
        year_month, bnf_presentation_code, region_name,
        bnf_id, region_id, period_id,
        icb_code, icb_name, prescription_month, financial_year
)
SELECT * FROM final