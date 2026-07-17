-- Custom generic tests. The built-in `relationships` test counts orphans in
-- ABSOLUTE terms — thresholds like error_if: >150 silently loosen as volume
-- grows (150 orphans in 10k orders is 1.5%; in 1M orders it's noise). These
-- express the same contracts as PROPORTIONS and volume bounds, which survive
-- scale changes.

-- Fails when the orphan RATE (child rows whose FK has no parent) exceeds
-- max_orphan_pct. Returns a single diagnostic row, so `failures` in the log
-- is 0 or 1 — the message carries the actual rate.
{% test ref_orphan_proportion(model, column_name, to, field, max_orphan_pct=1.5) %}
with orphans as (
    select count(*) as n
    from {{ model }} child
    left join {{ to }} parent
        on child.{{ column_name }} = parent.{{ field }}
    where child.{{ column_name }} is not null
      and parent.{{ field }} is null
),
total as (
    select count(*) as n from {{ model }}
)
select
    orphans.n as orphan_rows,
    total.n as total_rows,
    round(100.0 * orphans.n / nullif(total.n, 0), 3) as orphan_pct,
    {{ max_orphan_pct }} as max_orphan_pct
from orphans cross join total
where 100.0 * orphans.n / nullif(total.n, 0) > {{ max_orphan_pct }}
{% endtest %}

-- Volume test: a delivery that shrank or ballooned is wrong even if every
-- row in it is individually valid. Bounds come from the measured baseline.
{% test row_count_between(model, min_rows, max_rows) %}
select
    count(*) as row_count,
    {{ min_rows }} as min_rows,
    {{ max_rows }} as max_rows
from {{ model }}
having count(*) < {{ min_rows }} or count(*) > {{ max_rows }}
{% endtest %}
