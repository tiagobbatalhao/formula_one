create or replace view {project}.{dataset}.intfinalview_laps
OPTIONS()
as 
    with
    srcmain as (
        select * from {project}.{dataset}.int_session_laps
    ),
    final as (
        select
            event.id as event_id,
            session.id as session_id,
            lap.id as lap_id,
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
            STRUCT(
                lap.ReferenceTime as reference,
                DATE_ADD(
                    lap.ReferenceTime, INTERVAL CAST(
                        lap.SessionStartTime
                    *1000000 AS INT64) MICROSECOND
                ) as session_start,
                DATE_ADD(
                    lap.ReferenceTime, INTERVAL CAST(
                        lap.Time
                    *1000000 AS INT64) MICROSECOND
                ) as lap_end,
                lap.LapStartDate as lap_start,
                DATE_ADD(
                    lap.ReferenceTime, INTERVAL CAST(
                        lap.Sector1SessionTime
                    *1000000 AS INT64) MICROSECOND
                ) as sector1_end,
                DATE_ADD(
                    lap.ReferenceTime, INTERVAL CAST(
                        lap.Sector2SessionTime
                    *1000000 AS INT64) MICROSECOND
                ) as sector2_end,
                DATE_ADD(
                    lap.ReferenceTime, INTERVAL CAST(
                        lap.Sector3SessionTime
                    *1000000 AS INT64) MICROSECOND
                ) as sector3_end,
                DATE_ADD(
                    lap.ReferenceTime, INTERVAL CAST(
                        lap.PitOutTime
                    *1000000 AS INT64) MICROSECOND
                ) as pit_out,
                DATE_ADD(
                    lap.ReferenceTime, INTERVAL CAST(
                        lap.PitInTime
                    *1000000 AS INT64) MICROSECOND
                ) as pit_in
            ) as `timestamp`,
            STRUCT(
                lap.DriverNumber,
                lap.LapNumber,
                lap.Stint,
                lap.Team,
                lap.Driver,
                lap.LapTime,
                lap.Sector1Time,
                lap.Sector2Time,
                lap.Sector3Time,
                lap.SpeedI1,
                lap.SpeedI2,
                lap.SpeedFL,
                lap.SpeedST,
                lap.IsPersonalBest,
                lap.Compound,
                lap.TyreLife,
                lap.FreshTyre,
                lap.TrackStatus,
                lap.IsAccurate
            ) as `data`,
            uploaded_at
        from srcmain
        where event.id is not null
        and session.id is not null
        and lap.id is not null
    )
    select * from final
;
