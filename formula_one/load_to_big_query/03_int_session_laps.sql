create or replace view {project}.{dataset}.int_session_laps
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.stg_session_laps
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
                100000000000 + MOD(ABS(FARM_FINGERPRINT(CONCAT(
                    "E",
                    `event`.id,
                    "S",
                    `session`.id,
                    "D",
                    DriverNumber,
                    "L",
                    LapNumber
                ))), 900000000000) as `id`,
                DriverNumber,
                LapNumber,
                Stint,
                Team,
                Driver,
                ReferenceTime,
                SessionStartTime,
                `Time`,
                LapStartDate,
                LapStartTime,
                Sector1SessionTime,
                Sector2SessionTime,
                Sector3SessionTime,
                LapTime,
                Sector1Time,
                Sector2Time,
                Sector3Time,
                PitOutTime,
                PitInTime,
                SpeedI1,
                SpeedI2,
                SpeedFL,
                SpeedST,
                IsPersonalBest,
                Compound,
                TyreLife,
                FreshTyre,
                TrackStatus,
                IsAccurate
            ) as `lap`,
            uploaded_at
        from add_session_info
    )
    select * from final
;
