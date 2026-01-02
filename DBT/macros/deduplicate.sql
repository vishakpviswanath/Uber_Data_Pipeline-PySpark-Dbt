{% macro deduplicate(relation, partition_cols, order_col) %}

select *
from (
    select
        *,
        row_number() over (
            partition by {{ partition_cols | join(', ') }}
            order by {{ order_col }} desc
        ) as dedup_rank
    from {{ relation }}
) t
where dedup_rank = 1

{% endmacro %}
