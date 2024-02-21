create or replace view {project}.{dataset}.int_session_poslapdata
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.stg_session_poslapdata
    ),
    srcsessions as (
        select * from {project}.{dataset}.int_sessions
    ),
    srclaps as (
        select * from {project}.{dataset}.int_session_laps
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
    add_lap_info as (
        select
            ref.lap as `lap`,
            main.* except (DriverNumber, LapNumber)
        from add_session_info main
        left join srclaps ref
        on main.event.id = ref.event.id
        and main.session.id = ref.session.id
        and main.DriverNumber = ref.lap.DriverNumber
        and main.LapNumber = ref.lap.LapNumber
    ),
    final as (
        select
            `event`,
            `session`,
            `lap`,
            STRUCT(
                ReferenceTime,
                SessionStartTime,
                `Time`,
                `Date`,
                `SessionTime`
            ) as `time`,
            STRUCT(
                X,
                Y,
                Z,
                Source
            ) as `data`,
            uploaded_at
        from add_lap_info
    )
    select * from final
;
