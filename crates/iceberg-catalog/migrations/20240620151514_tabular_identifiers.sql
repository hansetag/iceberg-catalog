create type entity_type as enum ('table', 'view');

create table tabular_identifiers
(
    identifier_id uuid primary key default uuid_generate_v1mc(),
    namespace_id  uuid                            not null,
    name          Text collate "case_insensitive" not null,
    typ           entity_type                     not null,
    CONSTRAINT "unique_name_per_namespace_id" UNIQUE (namespace_id, name)
);

call add_time_columns('tabular_identifiers');
select trigger_updated_at('"tabular_identifiers"');

-- Insert all existing names from table into unique_identifiers
insert into tabular_identifiers (namespace_id, name, typ)
select namespace_id, table_name, 'table'
from "table";

-- Add identifier_id to table..

alter table "table"
    add column identifier_id uuid REFERENCES tabular_identifiers (identifier_id);

-- ..and fill it with the corresponding identifier_id
update "table"
set identifier_id = tabular_identifiers.identifier_id
from tabular_identifiers
where "table".namespace_id = tabular_identifiers.namespace_id
  and "table".table_name = tabular_identifiers.name;

-- ..and make it not null
alter table "table"
    alter column identifier_id set not null;

-- ..and drop namespace_id and table_name from table
alter table "table"
    drop column namespace_id,
    drop column table_name;