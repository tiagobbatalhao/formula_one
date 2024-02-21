create or replace view {project}.{dataset}.intfinalview_telemetry_car
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.int_session_carlapdata
    ),
    final as (
        select
            lap.id as lap_id,
            STRUCT(
              time.`Date` as recorded_at
            ) as timestamp,
            `data`,
            uploaded_at
        from srcmain
        where event.id is not null
        and session.id is not null
        and lap.id is not null
    )
    select * from final
;
