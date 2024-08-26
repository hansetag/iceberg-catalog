-- Add migration script here
create type table_format_version as enum ('1', '2');

alter table "table"
    add column table_format_version table_format_version;

UPDATE "table"
SET table_format_version = (metadata ->> 'format-version')::table_format_version
WHERE table_format_version IS NULL;

alter table "table"
    alter column table_format_version set not null;

create table table_schema
(
    schema_id int   not null,
    table_id  uuid  not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    schema    jsonb not null,
    CONSTRAINT "unique_schema_per_table" unique (table_id, schema_id),
    PRIMARY KEY (table_id, schema_id)
);

call add_time_columns('table_schema');
select trigger_updated_at('table_schema');

INSERT INTO table_schema (schema_id, table_id, schema)
SELECT (schema ->> 'schema-id')::int, table_id, schema
FROM "table",
     jsonb_array_elements(metadata -> 'schemas') AS schema;

create table table_current_schema
(
    table_id  uuid primary key REFERENCES "table" (table_id) ON DELETE CASCADE,
    schema_id int not null,
    FOREIGN KEY (table_id, schema_id) REFERENCES table_schema (table_id, schema_id)
);

call add_time_columns('table_current_schema');
select trigger_updated_at('table_current_schema');

INSERT INTO table_current_schema (table_id, schema_id)
SELECT table_id, (metadata -> 'current-schema-id')::int
FROM "table";

create table table_partition_spec
(
    partition_spec_id int   not null,
    table_id          uuid  not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    partition_spec    jsonb not null,
    CONSTRAINT "unique_partition_spec_per_table" unique (table_id, partition_spec_id),
    PRIMARY KEY (table_id, partition_spec_id)
);

call add_time_columns('table_partition_spec');
select trigger_updated_at('table_partition_spec');

INSERT INTO table_partition_spec (partition_spec_id, table_id, partition_spec)
SELECT (partition_spec ->> 'spec-id')::int, table_id, partition_spec
FROM "table",
     jsonb_array_elements(metadata -> 'partition-specs') partition_spec;

create table table_default_partition_spec
(
    table_id          uuid primary key REFERENCES "table" (table_id) ON DELETE CASCADE,
    partition_spec_id int not null,
    FOREIGN KEY (table_id, partition_spec_id) REFERENCES table_partition_spec (table_id, partition_spec_id)
);

call add_time_columns('table_default_partition_spec');
select trigger_updated_at('table_default_partition_spec');

INSERT INTO table_default_partition_spec (table_id, partition_spec_id)
SELECT table_id, (metadata -> 'default-spec-id')::int
FROM "table";

create table table_properties
(
    table_id uuid not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    key      text not null,
    value    text not null,
    PRIMARY KEY (table_id, key)
);

call add_time_columns('table_properties');
select trigger_updated_at('table_properties');

INSERT INTO table_properties (table_id, key, value)
SELECT table_id, key, value
FROM "table", jsonb_each_text(metadata -> 'properties');

create table table_snapshot
(
    snapshot_id        bigint not null primary key,
    table_id           uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    parent_snapshot_id bigint REFERENCES table_snapshot (snapshot_id),
    sequence_number    bigint not null,
    manifest_list      text   not null,
    summary            jsonb  not null,
    schema_id          int    not null,
    UNIQUE (table_id, snapshot_id)
);

call add_time_columns('table_snapshot');
select trigger_updated_at('table_snapshot');

INSERT INTO table_snapshot (snapshot_id, parent_snapshot_id, sequence_number, manifest_list, summary, schema_id,
                            table_id)
SELECT (snapshot ->> 'snapshot-id')::bigint,
       (snapshot ->> 'parent-snapshot-id')::bigint,
       (snapshot ->> 'sequence-number')::bigint,
       snapshot ->> 'manifest-list',
       snapshot -> 'summary',
       (snapshot ->> 'schema-id')::int,
       table_id
FROM "table",
     jsonb_array_elements(metadata -> 'snapshots') AS snapshot;


create table table_current_snapshot
(
    table_id    uuid PRIMARY KEY REFERENCES "table" (table_id) ON DELETE CASCADE,
    snapshot_id bigint not null,
    FOREIGN KEY (table_id, snapshot_id) REFERENCES table_snapshot (table_id, snapshot_id)
);

select trigger_updated_at('table_current_snapshot');
call add_time_columns('table_current_snapshot');

INSERT INTO table_current_snapshot (table_id, snapshot_id)
SELECT table_id, (metadata ->> 'current-snapshot-id')::bigint
FROM "table"
WHERE metadata ->> 'current-snapshot-id' IS NOT NULL
  AND (metadata ->> 'current-snapshot-id')::bigint != -1;

create table table_snapshot_log
(
    table_id    uuid   not null,
    snapshot_id bigint not null,
    timestamp   bigint not null,
    FOREIGN KEY (table_id, snapshot_id) REFERENCES table_snapshot (table_id, snapshot_id) ON DELETE CASCADE,
    PRIMARY KEY (table_id, snapshot_id)
);

call add_time_columns('table_snapshot_log');
select trigger_updated_at('table_snapshot_log');

INSERT INTO table_snapshot_log (table_id, snapshot_id, timestamp)
SELECT table_id,
       (snapshot ->> 'snapshot-id')::bigint,
       (snapshot ->> 'timestamp-ms')::bigint
FROM "table",
     jsonb_array_elements(metadata -> 'snapshots') AS snapshot;

create table table_metadata_log
(
    table_id      uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    timestamp     bigint not null,
    metadata_file text   not null,
    PRIMARY KEY (table_id, timestamp)
);

call add_time_columns('table_metadata_log');
select trigger_updated_at('table_metadata_log');

-- metadata log is a list of dictionaries in the metadata, we need to unflatten it and insert it into table_metadata_log
INSERT INTO table_metadata_log (table_id, timestamp, metadata_file)
SELECT table_id, (log ->> 'timestamp-ms')::bigint, log ->> 'metadata-file'
FROM "table",
     jsonb_array_elements(metadata -> 'metadata-log') AS log;

create table table_sort_orders
(
    sort_order_id int   not null,
    table_id      uuid  not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    sort_order    jsonb not null,
    PRIMARY KEY (table_id, sort_order_id)
);

call add_time_columns('table_sort_orders');
select trigger_updated_at('table_sort_orders');

INSERT INTO table_sort_orders (sort_order_id, table_id, sort_order)
SELECT (orders ->> 'order-id')::int, table_id, orders
FROM "table",
     jsonb_array_elements(metadata -> 'sort-orders') as orders;


create table table_default_sort_order_id
(
    table_id      uuid primary key REFERENCES "table" (table_id) ON DELETE CASCADE,
    sort_order_id int not null,
    FOREIGN KEY (table_id, sort_order_id) REFERENCES table_sort_orders (table_id, sort_order_id)
);

call add_time_columns('table_default_sort_order_id');
select trigger_updated_at('table_default_sort_order_id');

INSERT INTO table_default_sort_order_id (table_id, sort_order_id)
SELECT table_id, (metadata ->> 'default-sort-order-id')::int
FROM "table";

DROP TABLE IF EXISTS table_refs;


create table table_refs
(
    table_id                               uuid   not null REFERENCES "table" (table_id) ON DELETE CASCADE,
    table_ref_name                         text   not null,
    snapshot_id                            bigint not null,
    retention_branch_min_snapshots_to_keep int,
    retention_branch_max_snapshot_age_ms   bigint,
    retention_branch_max_ref_age_ms        bigint,

    retention_tag_max_ref_age_ms           bigint
);

call add_time_columns('table_refs');
select trigger_updated_at('table_refs');


INSERT INTO table_refs (table_id, snapshot_id, table_ref_name, retention_branch_min_snapshots_to_keep,
                        retention_branch_max_snapshot_age_ms, retention_branch_max_ref_age_ms,
                        retention_tag_max_ref_age_ms)
SELECT table_id,
       (ref ->> 'snapshot-id')::bigint,
       ref_id.key,
       CASE WHEN ref ->> 'type' = 'branch' THEN (ref ->> 'min-snapshots-to-keep')::int END,
       CASE WHEN ref ->> 'type' = 'branch' THEN (ref ->> 'max-snapshot-age-ms')::bigint END,
       CASE WHEN ref ->> 'type' = 'branch' THEN (ref ->> 'max-ref-age-ms')::bigint END,
       CASE WHEN ref ->> 'type' = 'tag' THEN (ref ->> 'max-ref-age-ms')::bigint END
FROM "table",
     jsonb_each(metadata -> 'refs') AS ref_id(key, ref);