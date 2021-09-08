create or replace view MODEL.VATSIM.TELEMETRY as (
    select 
        insert_at, 
        value:uid::varchar flight_id,
        value:cid::varchar user_id,
        value:name::varchar user_name,
        value:rating::varchar user_rating,
        value:callsign::varchar callsign,
        value:aircraft::varchar aircraft_type, 
        value:arr::varchar arrival_icao,
        value:dep::varchar dep_icao, 
        value:alt::varchar::int altitude, 
        value:crzalt::varchar cruise_altitude,
        value:gndspd::varchar::int ground_speed,
        value:hdg::varchar::int heading,
        value:lat::varchar lat,
        value:lon::varchar lon,
        value:route::varchar route
    from lake.dataflow.vatsim, lateral flatten( input => payload::variant::object:payload ) vm
);