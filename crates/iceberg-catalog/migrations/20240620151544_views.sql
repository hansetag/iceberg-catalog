create type view_format_version as enum ('v1');

create table view
(
    view_id             uuid primary key default uuid_generate_v1mc(),
    CONSTRAINT "tabular_ident_fk" FOREIGN KEY (view_id) REFERENCES tabular (tabular_id),
    view_format_version view_format_version not null,
    -- Speed up S3 Signing requests. Otherwise not needed
    -- as the location is stored in the metadata.
    location            text                not null,
    metadata_location   text                not null
);
call add_time_columns('"view"');
select trigger_updated_at('"view"');


create table view_properties
(
    property_id uuid primary key default uuid_generate_v1mc(),
    view_id     uuid not null REFERENCES view (view_id) ON DELETE CASCADE,
    key         text not null,
    value       text not null
);


call add_time_columns('view_properties');
select trigger_updated_at('"view_properties"');

create table view_schema
(
    schema_uuid uuid primary key default uuid_generate_v1mc(),
    schema_id   int   not null,
    view_id     uuid  not null REFERENCES view (view_id) ON DELETE CASCADE,
    -- the schema object is quite complex and I'm not sure about the benefits of inviting that complexity into sql
    schema      jsonb not null,
    CONSTRAINT "unique_schema_per_view" unique (view_id, schema_id)
);
call add_time_columns('view_schema');
select trigger_updated_at('view_schema');



create table view_version
(
    view_version_uuid uuid   not null primary key default uuid_generate_v1mc(),
    view_id           uuid   not null REFERENCES view (view_id) ON DELETE CASCADE,
    version_id        bigint not null,
    schema_id         int    not null,
    version           jsonb  not null,
    FOREIGN KEY (view_id, schema_id) REFERENCES view_schema (view_id, schema_id) ON DELETE CASCADE,
    constraint "unique_version_per_metadata" unique (view_id, version_id),
    constraint "unique_version_per_metadata_including_pkey" unique (view_version_uuid, view_id, version_id)
);
call add_time_columns('view_version');
select trigger_updated_at('view_version');

create table current_view_metadata_version
(
    view_id      uuid primary key REFERENCES view (view_id) ON DELETE CASCADE,
    version_uuid uuid not null REFERENCES view_version (view_version_uuid) ON DELETE CASCADE,
    version_id   int8 not null,
    FOREIGN KEY (version_uuid, view_id, version_id) REFERENCES view_version (view_version_uuid, view_id, version_id)
);

call add_time_columns('current_view_metadata_version');
select trigger_updated_at('"current_view_metadata_version"');

create table view_version_log
(
    id         uuid primary key default uuid_generate_v1mc(),
    view_id    uuid        not null,
    version_id bigint      not null,
    timestamp  timestamptz not null,
    FOREIGN KEY (view_id, version_id) REFERENCES view_version (view_id, version_id) ON DELETE CASCADE
);
call add_time_columns('view_version_log');

