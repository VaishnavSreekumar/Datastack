-- Reusable date macros for PostgreSQL and SQL warehouse layers

-- Format date from timestamp/date to DD/MM/YYYY
{% macro format_date(date_column) %}
    to_char(cast({{ date_column }} as date), 'DD/MM/YYYY')
{% endmacro %}

-- Extract month as integer (1-12)
{% macro extract_month(date_column) %}
    extract(month from cast({{ date_column }} as date))
{% endmacro %}

-- Extract year as integer (e.g. 2017)
{% macro extract_year(date_column) %}
    extract(year from cast({{ date_column }} as date))
{% endmacro %}
