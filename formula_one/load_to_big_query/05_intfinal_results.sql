create or replace view {project}.{dataset}.intfinalview_results
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.int_session_results
    ),
    final as (
        select
            event.id as event_id,
            session.id as session_id,
            STRUCT(
                event.Year as year,
                event.RoundNumber as round_number,
                event.Country as country,
                event.Location as location,
                event.EventName as short_name,
                event.OfficialEventName as official_name,
                event.EventFormat as format,
                event.EventDate as date,
                event.F1ApiSupport as f1api_support
            ) as `event`,
            STRUCT(
                session.SessionType as type,
                session.SessionIndex as index,
                session.SessionName as name,
                session.SessionDate as date
            ) as `session`,
            `driver`,
            `team`,
            `data`,
            uploaded_at
        from srcmain
        where event.id is not null
        and session.id is not null
    )
    select * from final
;
