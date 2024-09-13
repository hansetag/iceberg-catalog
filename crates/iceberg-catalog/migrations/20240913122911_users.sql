create table users
(
    id         uuid primary key,
    name       text not null,
    email      text not null,
    deleted_at timestamptz
);

-- TODO: project table with user id? Could be useful for tracking who created the project, could also offload that to openfga.

call add_time_columns('users');
select trigger_updated_at('"users"');
