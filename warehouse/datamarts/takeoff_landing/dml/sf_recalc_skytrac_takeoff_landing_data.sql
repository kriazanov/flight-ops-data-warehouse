drop function dwh_staging.sf_recalc_skytrac_takeoff_landing_data();
create or replace function dwh_staging.sf_recalc_skytrac_takeoff_landing_data()
returns void
language plpgsql
as $$
begin
    -- updating id's
    update dwh_presentation.fact_takeoff_landing ftl
    set id_fact_leg = (
        select fl.id
        from dwh_presentation.fact_leg fl
        where fl.id_board = ftl.id_board
        and case ftl.id_takeoff_landing_kind
                when 1 /*takeoff*/ then fl.id_origin
                when 2 /*landing*/ then fl.id_destination
            end = ftl.id_airport
        order by
            case ftl.id_takeoff_landing_kind
                when 1 /*takeoff*/ then abs(extract(epoch from ftl.datetime - departure_fact_utc))
                when 2 /*landing*/ then abs(extract(epoch from ftl.datetime - arrival_fact_utc))
            end
        limit 1)
    where ftl.id_fact_leg is null;

    update dwh_presentation.fact_takeoff_landing ftl
    set id_flight = fl.id_flight, id_captain = dctm.id
    from dwh_presentation.fact_leg fl
    left join dwh_presentation.fact_crewtask fct on fct.md5_flight = fl.md5_flight
    left join dwh_presentation.dim_crew_task_member dctm on dctm.id = fct.id_crew_task_member
    where fl.id = ftl.id_fact_leg
    and dctm.role = 'КВС'
    and id_captain is null
    and id_fact_leg is not null;

    -- calculating facts
    update dwh_staging.fact_takeoff_landing u
    set distance_m = st_distancesphere(st_point(f.longitude, f.latitude), st_point(d.runway_end_long, d.runway_end_lat))
    from dwh_staging.fact_takeoff_landing f
    inner join dwh_presentation.dim_runway d on d.id = f.id_runway
    where u.id = f.id
    and u.id_fact_leg is not null;

end$$;