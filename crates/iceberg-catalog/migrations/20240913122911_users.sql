create type user_origin as enum ('explicit-via-register-call', 'implicit-via-config-call');

create table users
(
    id         text primary key, -- url-encoded(issuer-url)/(oid|sub-alphanumeric)
    name       text        not null,
    email      text,
    origin     user_origin not null,
    deleted_at timestamptz
);

call add_time_columns('users');
select trigger_updated_at('users');

create table roles
(
    id          text primary key,
    name        text not null,
    description text
);

call add_time_columns('roles');
select trigger_updated_at('roles');

