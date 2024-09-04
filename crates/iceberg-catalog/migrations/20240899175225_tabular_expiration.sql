create type task_status as enum ('pending', 'running', 'done', 'failed', 'cancelled');

create table task
(
    task_id            uuid primary key,
    warehouse_id       uuid              not null,     -- the warehouse that the task is associated with
    idempotency_key    uuid              not null,     -- key to ensure idempotency
    task_name          text              not null,     -- name of the task queue
    details            jsonb,                          -- additional details about the task, not structured
    status             task_status       not null,
    last_error_details text,                           -- details about the error if the task failed
    picked_up_at       timestamptz,                    -- when the task was picked up by a worker
    suspend_until      timestamptz,                    -- when the task should be retried
    attempt            integer default 0 not null,     -- how many times the task has been attempted
    parent_task_id     uuid REFERENCES task (task_id), -- the task that spawned this task
    CONSTRAINT unique_idempotency_key UNIQUE (idempotency_key, task_name)
);

create index task_name_idx on task (task_name);
create index task_warehouse_idx on task (warehouse_id);
create index task_name_status_idx on task (task_name, status) where status = 'pending' OR status = 'running';


create table tabular_expirations
(
    tabular_id    uuid          not null,
    warehouse_id  uuid          not null,
    typ           tabular_type  not null,
    deletion_kind deletion_kind not null,
    task_id       uuid primary key references task (task_id)
);

alter table tabular
    drop column deletion_kind;

create table tabular_purges
(
    tabular_id   uuid         not null,
    warehouse_id uuid         not null,
    typ          tabular_type not null,
    task_id      uuid primary key references task (task_id)
);