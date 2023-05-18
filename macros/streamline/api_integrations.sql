{% macro create_aws_terra_api() %}
    {{ log("Creating integration for target:" ~ target) }}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_terra_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-terra' api_allowed_prefixes = (
            '<REPLACE_WITH_PROD_URI>'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_terra_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-terra' api_allowed_prefixes = (
            'https://o1q7yct12j.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "sbx" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_terra_api_sbx_shah api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::579011195466:role/snowflake-api-terra' api_allowed_prefixes = (
            'https://33fgv8p4d4.execute-api.us-east-1.amazonaws.com/sbx/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
