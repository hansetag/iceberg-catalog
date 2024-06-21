create type entity_type as enum ('table', 'view');

create table tabular
(
    -- view or table id
    tabular_id   uuid primary key,
    namespace_id uuid                            not null references namespace (namespace_id),
    name         Text collate "case_insensitive" not null,
    typ          entity_type                     not null,
    CONSTRAINT "unique_name_per_namespace_id" UNIQUE (namespace_id, name)
);

call add_time_columns('tabular');
select trigger_updated_at('tabular');

-- Insert all existing names from table into unique_identifiers
insert into tabular (tabular_id, namespace_id, name, typ)
select table_id, namespace_id, table_name, 'table'
from "table";


-- ..and drop namespace_id and table_name from table
alter table "table"
    add constraint "tabular_ident_fk" foreign key (table_id) references tabular (tabular_id) on update cascade,
    drop column namespace_id,
    drop column table_name;
