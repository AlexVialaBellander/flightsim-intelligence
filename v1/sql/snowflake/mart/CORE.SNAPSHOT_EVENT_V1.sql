create or replace view MART.CORE.SNAPSHOT_EVENT_V1 as (
    select 
        convert_timezone('UTC', created_at)::timestamp_ntz(0) as created_at_utc,
        'ivao' as network_operator,
        'ivao_' || id as flight_id,
        callsign,
        'ivao_' || user_id as user_id,
        arrival_icao,
        dep_icao as departure_icao,
        lat,
        lon,
        altitude,
        heading,
        ground_speed
    from 
        model.ivao.dataflow_snapshot
    union
    select
        convert_timezone('UTC', insert_at)::timestamp_ntz(0) as created_at_utc,
        'vatsim' as network_operator,
        'vatsim_' || flight_id as flight_id,
        callsign,
        'vatsim_' || user_id as user_id,
        arrival_icao,
        dep_icao as departure_icao,
        lat::float,
        lon::float,
        altitude,
        heading,
        ground_speed
    from
        model.vatsim.telemetry
);