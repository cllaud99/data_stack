{% macro parse_date_br(col) %}
    CASE
        WHEN NULLIF(TRIM({{ col }}), '00000000') IS NULL THEN NULL
        ELSE TO_DATE(TRIM({{ col }}), 'YYYYMMDD')
    END
{% endmacro %}
