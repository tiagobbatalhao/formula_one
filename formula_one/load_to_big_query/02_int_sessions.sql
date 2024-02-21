create or replace view {project}.{dataset}.int_sessions
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.stg_schedule
    ),
    src1 as (
        select
            `Year`, RoundNumber, Country, `Location`, `OfficialEventName`,
            EventDate, EventName, EventFormat, F1ApiSupport, uploaded_at,
            Session1 as SessionName,
            Session1Date as SessionDate,
            1 as SessionIndex,
        from srcmain
    ),
    src2 as (
        select
            `Year`, RoundNumber, Country, `Location`, `OfficialEventName`,
            EventDate, EventName, EventFormat, F1ApiSupport, uploaded_at,
            Session2 as SessionName,
            Session2Date as SessionDate,
            2 as SessionIndex,
        from srcmain
    ),
    src3 as (
        select
            `Year`, RoundNumber, Country, `Location`, `OfficialEventName`,
            EventDate, EventName, EventFormat, F1ApiSupport, uploaded_at,
            Session3 as SessionName,
            Session3Date as SessionDate,
            3 as SessionIndex,
        from srcmain
    ),
    src4 as (
        select
            `Year`, RoundNumber, Country, `Location`, `OfficialEventName`,
            EventDate, EventName, EventFormat, F1ApiSupport, uploaded_at,
            Session4 as SessionName,
            Session4Date as SessionDate,
            4 as SessionIndex,
        from srcmain
    ),
    src5 as (
        select
            `Year`, RoundNumber, Country, `Location`, `OfficialEventName`,
            EventDate, EventName, EventFormat, F1ApiSupport, uploaded_at,
            Session5 as SessionName,
            Session5Date as SessionDate,
            5 as SessionIndex,
        from srcmain
    ),
    unioned as (
        select * from src1
        union all select * from src2
        union all select * from src3
        union all select * from src4
        union all select * from src5
    ),
    final as (
        select
            STRUCT(
                100000000000 + MOD(ABS(FARM_FINGERPRINT(CONCAT(
                    "Y",
                    `Year`,
                    "N",
                    EventName
                ))), 900000000000) as `id`,
                CAST(`Year` AS INT64) as `Year`,
                CAST(RoundNumber AS INT64) AS RoundNumber,
                IF(LENGTH(`Country`)>0, `Country`, NULL) AS Country,
                IF(LENGTH(`Location`)>0, `Location`, NULL) AS `Location`,
                IF(LENGTH(`EventName`)>0, `EventName`, NULL) AS `EventName`,
                IF(LENGTH(`OfficialEventName`)>0, `OfficialEventName`, NULL) AS `OfficialEventName`,
                IF(LENGTH(`EventFormat`)>0, `EventFormat`, NULL) AS `EventFormat`,
                DATETIME(EventDate) as EventDate,
                CAST(F1ApiSupport AS BOOL) AS F1ApiSupport
            ) as `event`,
            STRUCT(
                100000000000 + MOD(ABS(FARM_FINGERPRINT(CONCAT(
                    "Y",
                    `Year`,
                    "N",
                    EventName,
                    "I",
                    SessionIndex
                ))), 900000000000) as `id`,
                CASE
                    WHEN SessionName IN ('Practice 1') THEN 'FP1'
                    WHEN SessionName IN ('Practice 2') THEN 'FP2'
                    WHEN SessionName IN ('Practice 3') THEN 'FP3'
                    WHEN SessionName IN ('Sprint', 'Sprint Qualifying') THEN 'S'
                    WHEN SessionName IN ('Qualifying') THEN 'Q'
                    WHEN SessionName IN ('Race') THEN 'R'
                END as SessionType,
                CAST(SessionIndex AS INT64) AS SessionIndex,
                IF(LENGTH(`SessionName`)>0, `SessionName`, NULL) AS `SessionName`,
                DATETIME(SessionDate) as SessionDate
            ) as `session`,
            uploaded_at
        from unioned
    )
    select * from final
    where (session.SessionName != 'None')
    or (session.SessionDate is not null)
;