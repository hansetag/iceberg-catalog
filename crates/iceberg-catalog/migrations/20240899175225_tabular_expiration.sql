create type task_status as enum ('pending', 'running', 'done', 'failed');

create table task
(
    task_id         uuid primary key,
    idempotency_key uuid              not null,     -- key to ensure idempotency
    task_name       text              not null,     -- name of the task queue
    details         jsonb,                          -- additional details about the task, not structured
    status          task_status       not null,
    picked_up_at    timestamptz,                    -- when the task was picked up by a worker
    suspend_until   timestamptz,                    -- when the task should be retried
    attempt         integer default 0 not null,     -- how many times the task has been attempted
    parent_task_id  uuid REFERENCES task (task_id), -- the task that spawned this task
    CONSTRAINT unique_idempotency_key UNIQUE (idempotency_key)
);

create index task_name_idx on task (task_name);
create index task_name_status_idx on task (task_name, status) where status = 'pending' OR status = 'running';


create table tabular_expirations
(
    tabular_id   uuid         not null,
    warehouse_id uuid         not null,
    typ          tabular_type not null,
    task_id      uuid primary key references task (task_id)
);

create table deletions
(
    entity_id    uuid not null,
    warehouse_id uuid not null,
    location     text not null,
    task_id      uuid primary key references task (task_id)
);



alter table deletions
    add constraint unique_location_per_warehouse unique (warehouse_id, location);