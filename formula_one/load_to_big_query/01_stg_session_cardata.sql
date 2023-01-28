create or replace view {project}.{dataset}.stg_session_cardata
OPTIONS()
as 
    with
    src as (
        select * from 
        {project}.{dataset}.src_session_cardata
    ),
    final as (
        select * from src
        qualify rank() over (
            partition by EventYear, EventName, SessionType
            order by uploaded_at desc
        ) = 1
    )
    select * from final
;