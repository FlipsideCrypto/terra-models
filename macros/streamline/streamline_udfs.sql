{% macro create_udf_get_chainhead() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_avalanche_api AS {% if target.name == "prod" %}
        '<REPLACE_WITH_PROD_URI>/get_chainhead'
    {% else %}
        'https://33fgv8p4d4.execute-api.us-east-1.amazonaws.com/sbx/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = aws_avalanche_api AS {% if target.name == "prod" %}
        '<REPLACE_WITH_PROD_URI>/udf_bulk_json_rpc'
    {% else %}
        'https://33fgv8p4d4.execute-api.us-east-1.amazonaws.com/sbx/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}



