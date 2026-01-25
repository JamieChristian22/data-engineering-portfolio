{% macro scd2_closeout(this_table, natural_key, hash_col) %}
update {{ this_table }} as t
set valid_to = current_timestamp,
    is_current = false
from (
  select {{ natural_key }} as nk, max(valid_from) as newest_from
  from {{ this_table }}
  group by 1
) as x
where t.{{ natural_key }} = x.nk
  and t.is_current = true
  and t.valid_from < x.newest_from;
{% endmacro %}
