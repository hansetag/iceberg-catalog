create table users
(
    id           uuid primary key,
    name         text,
    display_name text,
    email        text,
    origin       text not null, -- either registered or the endpoint via which the user was implicitly created, TODO: @christian enum?
    deleted_at   timestamptz
);

-- TODO: project table with user id? Could be useful for tracking who created the project, could also offload that to openfga.

call add_time_columns('users');
select trigger_updated_at('"users"');
