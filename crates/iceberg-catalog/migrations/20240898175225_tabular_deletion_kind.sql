create type deletion_kind as enum ('default', 'purge');

alter table tabular
    add column deletion_kind deletion_kind;