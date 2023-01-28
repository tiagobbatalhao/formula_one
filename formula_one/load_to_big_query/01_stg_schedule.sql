create or replace view {project}.{dataset}.stg_schedule
OPTIONS()
as 
    with
    src as (
        select * from 
        {project}.{dataset}.src_schedule
    ),
    final as (
        select * from src
        qualify rank() over (
            partition by `Year`
            order by uploaded_at desc
        ) = 1
    )
    select * from final
;