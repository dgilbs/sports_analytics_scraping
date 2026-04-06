with transfers as (
    select
        manager,
        player_out,
        player_in,
        transfer_date::date as transfer_date,
        is_sei::boolean     as is_sei,
        is_d45::boolean     as is_d45,
        is_transfer_window::boolean as is_transfer_window,
        case
            when is_transfer_window::boolean or is_sei::boolean then 0
            when is_d45::boolean                                 then -10
            else                                                      -20
        end as penalty
    from {{ ref('transfers_2026') }}
)

select
    manager,
    count(*)                                    as total_transfers,
    count(*) filter (where penalty = 0)         as free_transfers,
    count(*) filter (where penalty = -10)       as d45_transfers,
    count(*) filter (where penalty = -20)       as regular_transfers,
    sum(penalty)                                as transfer_penalty
from transfers
group by manager
order by transfer_penalty
