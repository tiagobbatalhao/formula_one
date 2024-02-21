create or replace view {project}.{dataset}.int_session_weather
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.stg_session_weather
    ),
    srcsessions as (
        select * from {project}.{dataset}.int_sessions
    ),
    add_session_info as (
        select
            ref.event as `event`,
            ref.session as `session`,
            main.* except (EventYear, EventName, SessionType)
        from srcmain main
        left join srcsessions ref
        on main.EventYear = ref.event.Year
        and main.EventName = ref.event.EventName
        and main.SessionType = ref.session.SessionType
    ),
    final as (
        select
            `event`,
            `session`,
            STRUCT(
                ReferenceTime,
                SessionStartTime,
                `Time`
            ) as `time`,
            STRUCT(
                AirTemp,	
                Humidity,
                Pressure,
                Rainfall,
                TrackTemp,
                WindDirection,
                WindSpeed
            ) as `data`,
            uploaded_at
        from add_session_info
    )
    select * from final
;

