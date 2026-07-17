-- on-run-end hook: persist every test result to a queryable Delta table.
-- The job UI tells you a run failed; this table tells you WHAT has been
-- failing, on WHICH table/column, over TIME:
--
--   select * from workspace.lakehouse_demo.qc_log
--   where status != 'pass' order by logged_at desc;
--
-- The reconcile notebook (04) writes its checks to the same table, so one
-- query covers the whole pipeline's QC history.
{% macro log_qc_results(results) %}
    {% if execute and results | length > 0 %}
        {% set log_table = target.catalog ~ '.' ~ target.schema ~ '.qc_log' %}
        {% do run_query(
            "create table if not exists " ~ log_table ~ " ("
            ~ "  logged_at timestamp,"
            ~ "  run_date date,"
            ~ "  invocation_id string,"
            ~ "  source string,"
            ~ "  check_name string,"
            ~ "  table_name string,"
            ~ "  column_name string,"
            ~ "  status string,"
            ~ "  failures bigint,"
            ~ "  execution_time_s double,"
            ~ "  message string"
            ~ ") using delta"
        ) %}

        {% set rows = [] %}
        {% for result in results %}
            {% set node = result.node %}
            {% if node.resource_type == 'test' %}
                {# The table under test. attached_node is None for source tests,
                   and depends_on order is template-dependent — the reliable
                   record is the test's own `model` kwarg, e.g.
                   "{{ get_where_subquery(source('lakehouse_demo', 'silver_orders')) }}". #}
                {% set tm = node.test_metadata if node.test_metadata is defined else none %}
                {% set kw_model = (tm.kwargs.get('model') or '') if tm else '' %}
                {% if "source(" in kw_model %}
                    {% set table_name = kw_model.split("'")[3] %}
                {% elif "ref(" in kw_model %}
                    {% set table_name = kw_model.split("'")[1] %}
                {% elif node.attached_node %}
                    {% set table_name = node.attached_node.split('.')[-1] %}
                {% else %}
                    {% set table_name = '' %}
                {% endif %}
                {% set column_name = node.column_name or '' %}
                {% set failures = result.failures if result.failures is not none else 0 %}
                {% set message = (result.message or '') | replace("'", "") | truncate(300) %}
                {% do rows.append(
                    "(current_timestamp(), current_date(), '" ~ invocation_id ~ "', 'dbt', '"
                    ~ node.name ~ "', '" ~ table_name ~ "', '" ~ column_name ~ "', '"
                    ~ result.status ~ "', " ~ failures ~ ", "
                    ~ (result.execution_time or 0) ~ ", '" ~ message ~ "')"
                ) %}
            {% endif %}
        {% endfor %}

        {% if rows | length > 0 %}
            {% do run_query(
                "insert into " ~ log_table
                ~ " (logged_at, run_date, invocation_id, source, check_name, table_name,"
                ~ "  column_name, status, failures, execution_time_s, message) values "
                ~ rows | join(', ')
            ) %}
        {% endif %}
    {% endif %}
{% endmacro %}
