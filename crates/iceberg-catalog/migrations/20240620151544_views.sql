create type view_format_version as enum ('v1');


create table view_metadata
(
    metadata_id         uuid primary key default uuid_generate_v1mc(),
    metadata_location   text                not null,
    view_format_version view_format_version not null
);

call add_time_columns('view_metadata');
select trigger_updated_at('"view_metadata"');

create table view_metadata_versions
(
    id          uuid        not null primary key default uuid_generate_v1mc(),
    metadata_id uuid        not null REFERENCES view_metadata (metadata_id),
    version_id  int         not null,
    schema_id   int         not null,
    timestamp   timestamptz not null,
    constraint "unique_version_per_metadata" unique (metadata_id, version_id)
);

call add_time_columns('view_metadata_versions');
select trigger_updated_at('"view_metadata_versions"');

create table "view"
(
    view_id       uuid primary key     default uuid_generate_v1mc(),
    identifier_id uuid        not null REFERENCES tabular_identifiers (identifier_id),
    metadata_id   uuid        not null REFERENCES view_metadata (metadata_id),
    -- Speed up S3 Signing requests. Otherwise not needed
    -- as the location is stored in the metadata.
    view_location text        not null,
    created_at    timestamptz not null default now(),
    updated_at    timestamptz
);

call add_time_columns('"view"');
select trigger_updated_at('"view"');


create table current_view_metadata_version
(
    metadata_id uuid primary key REFERENCES view_metadata (metadata_id),
    version_id  uuid not null REFERENCES view_metadata_versions (id)
);

call add_time_columns('current_view_metadata_version');
select trigger_updated_at('"current_view_metadata_version"');

create table metadata_summary
(
    metadata_id uuid primary key REFERENCES view_metadata (metadata_id),
    version_id  uuid not null REFERENCES view_metadata_versions (id),
    key         text not null,
    value       text not null
);

call add_time_columns('metadata_summary');
select trigger_updated_at('"metadata_summary"');

create table view_metadata_version_representations
(
    view_representation_id uuid primary key default uuid_generate_v1mc(),
    metadata_id            uuid not null REFERENCES view_metadata (metadata_id),
    version_id             uuid not null REFERENCES view_metadata_versions (id)
);

call add_time_columns('view_metadata_version_representations');
select trigger_updated_at('"view_metadata_version_representations"');

create table sql_view_representation
(
    view_representation_id uuid primary key     default uuid_generate_v1mc(),
    sql                    text        not null,
    dialect                text        not null,
    created_at             timestamptz not null default now(),
    updated_at             timestamptz
);

call add_time_columns('sql_view_representation');
select trigger_updated_at('"sql_view_representation"');