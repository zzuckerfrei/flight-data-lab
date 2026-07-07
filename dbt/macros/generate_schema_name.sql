{#
  레이어별 데이터셋(bronze/staging/marts) + dev/prod 환경 분리.

  기본 dbt는 custom schema를 `{{ target.schema }}_{{ custom_schema }}`로 만든다.
  이를 오버라이드해:
   - custom schema 없음(예: source)      → target 기본 데이터셋(flight_data)
   - target=prod                          → custom schema 그대로   (staging, marts)  ← 운영
   - 그 외(dev 등)                        → {target.name}_{custom}  (dev_staging, dev_marts)  ← 개발, prod 안 밟음

  → dev로 돌리면 dev_* 데이터셋, prod로 돌리면 깨끗한 이름. 환경 격리(현업 표준).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.name }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
