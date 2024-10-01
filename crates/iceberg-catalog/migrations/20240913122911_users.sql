create type user_origin as enum ('explicit-via-register-call', 'implicit-via-config-call');

create table users
(
    id         text primary key, -- url-encoded(issuer-url)/(oid|sub-alphanumeric)
    name       text        not null,
    email      text,
    origin     user_origin not null,
    deleted_at timestamptz
);


-- TODO: project table with user id? Could be useful for tracking who created the project, could also offload that to openfga.

call add_time_columns('users');
select trigger_updated_at('"users"');
