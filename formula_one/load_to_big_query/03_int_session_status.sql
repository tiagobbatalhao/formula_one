create or replace view {project}.{dataset}.int_session_status
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.stg_session_status
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
                IF(LENGTH(`Status`)>0, `Status`, NULL) AS `Status`
            ) as `data`,
            uploaded_at
        from add_session_info
    )
    select * from final
;

