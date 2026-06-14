WITH source AS (
    SELECT
        bnf_presentation_code,
        bnf_presentation_name,
        bnf_chemical_substance,
        bnf_chapter,
        ROW_NUMBER() OVER (
            PARTITION BY bnf_presentation_code
            ORDER BY bnf_presentation_name
        ) AS rn
    FROM {{ source('silver', 'prescriptions_clean') }}
    WHERE bnf_presentation_code IS NOT NULL
),
deduped AS (
    SELECT
        bnf_presentation_code,
        bnf_presentation_name,
        bnf_chemical_substance,
        bnf_chapter
    FROM source
    WHERE rn = 1
),
final AS (
    SELECT
        CONVERT(VARCHAR(32), HASHBYTES('MD5',
            CAST(bnf_presentation_code AS NVARCHAR(255))), 2)  AS bnf_id,
        bnf_presentation_code                                   AS bnf_code,
        bnf_presentation_name                                   AS bnf_description,
        bnf_chemical_substance                                  AS bnf_chemical_substance,
        bnf_chapter                                             AS bnf_chapter_name,
        LEFT(bnf_presentation_code, 2)                          AS bnf_chapter_code,
        LEFT(bnf_presentation_code, 4)                          AS bnf_section_code
    FROM deduped
)
SELECT * FROM final