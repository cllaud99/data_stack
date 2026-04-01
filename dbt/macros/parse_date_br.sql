{% macro parse_date_br(col) %}
    CASE
        WHEN NULLIF(TRIM({{ col }}), '') IS NULL THEN NULL
        WHEN LENGTH(TRIM({{ col }})) != 8  THEN NULL
        WHEN TRIM({{ col }}) = '00000000'  THEN NULL
        ELSE TO_DATE(TRIM({{ col }}), 'YYYYMMDD')
    END
{% endmacro %}
