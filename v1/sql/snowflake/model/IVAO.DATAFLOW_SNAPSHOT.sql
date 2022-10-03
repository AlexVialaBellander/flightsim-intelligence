create or replace view MODEL.IVAO.DATAFLOW_SNAPSHOT as (
    select 
        value:callsign::varchar callsign,
        insert_at as created_at,
        value:flightPlan:arrivalId::varchar arrival_icao,
        value:flightPlan:departureId::varchar dep_icao,
        value:lastTrack:latitude::float lat,
        value:lastTrack:longitude::float lon,
        value:lastTrack:altitude::number altitude,
        value:lastTrack:heading::number heading,
        value:lastTrack:groundSpeed::number ground_speed,
        value:id::varchar id,
        value:lastTrack last_track_obj,
        value:pilotSession pilot_session_obj,
        value:rating::int rating,
        value:serverId::varchar server_id,
        value:softwareTypeId::varchar software_type_id,
        value:softwareVersion::varchar software_version,
        value:time::int time,
        value:userId::int user_id
    from lake.dataflow.ivao, lateral flatten( input => payload::variant::object:payload:clients:pilots) vm
);