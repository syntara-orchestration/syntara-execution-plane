# Execution Plane: Work Executor

The Work Executor is the entry point for work submission into the EP. The Consumer
(Syntara or AWX) calls it to create a `WorkItem` and persist it through the [`Work boundary`](work-store.md).

See [logical_components.md](logical_components.md) for how it fits into the logical decomposition of the Execution Plane service.

---

## Responsibility

Accepts a work submission from the Consumer — including a caller-generated UUID and the
work payload — validates it, and writes a `WorkItem` record through the [`Work boundary`](work-store.md) in
`PENDING` status. Returns to the Consumer immediately; execution is asynchronous.

The UUID is caller-owned. The Consumer generates it before submitting so it can track
the work item without waiting for a response.

The Work Scheduler claims and places the `WorkItem`, then dispatches it via a
`WorkerManager`. The manager records the result through `WorkStore`; the
Completion Notifier delivers the outcome to the consumer. See
[architecture.md](architecture.md) for the full execution flow.

---

## Submission interface

AO submits `work.execute` through `POST /v1/commands`. The command handler
calls the [Work boundary](work-store.md), which persists the `WorkItem` and
sends `pg_notify` on `execution_plane_work_items` to wake the worker. See
[integration.md](integration.md) for the command envelope and authentication.

## Validation

The Work command handler validates the command payload before the Service
persists it. The database schema also enforces the persisted structure.
