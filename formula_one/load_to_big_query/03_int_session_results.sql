create or replace view {project}.{dataset}.int_session_results
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.stg_session_results
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
                CAST(DriverNumber AS INT64) AS `DriverNumber`,
                IF(LENGTH(`BroadcastName`)>0, `BroadcastName`, NULL) AS `BroadcastName`,
                IF(LENGTH(`Abbreviation`)>0, `Abbreviation`, NULL) AS `Abbreviation`,
                IF(LENGTH(`FirstName`)>0, `FirstName`, NULL) AS `FirstName`,
                IF(LENGTH(`LastName`)>0, `LastName`, NULL) AS `LastName`,
                IF(LENGTH(`FullName`)>0, `FullName`, NULL) AS `FullName`
            ) as `driver`,
            STRUCT(
                IF(LENGTH(`TeamName`)>0, `TeamName`, NULL) AS TeamName,
                IF(LENGTH(`TeamColor`)>0, `TeamColor`, NULL) AS TeamColor
            ) as `team`,
            STRUCT(
                CAST(Position AS INT64) AS `Position`,
                CAST(GridPosition AS INT64) AS `GridPosition`,
                CAST(Q1 AS FLOAT64) AS `Q1`,
                CAST(Q2 AS FLOAT64) AS `Q2`,
                CAST(Q3 AS FLOAT64) AS `Q3`,
                CAST(`Time` AS FLOAT64) AS `Time`,
                CAST(`Points` AS FLOAT64) AS `Points`,
                IF(LENGTH(`Status`)>0, `Status`, NULL) AS `Status`
            ) as `data`,
            uploaded_at
        from add_session_info
    )
    select * from final
;