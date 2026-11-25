# Neon pooler: ParseComplete written into the middle of a RowDescription

A reproduction of a protocol corruption seen through Neon's pooled endpoint when a client pipelines `Parse` for several named prepared statements that the pooler's backend connection already has.

**Status:** reproduced on three different pooled endpoints on 2026-11-25. Not yet confirmed by Neon. The pooler is PgBouncer in transaction mode; whether this is specific to Neon's build or also in upstream PgBouncer is not known.

## What happens

A client that pipelines `Parse` + `Describe` for several named statements, then one `Sync`, expects `ParseComplete`, `ParameterDescription`, `RowDescription` back for each.

If the backend connection it is given already has those statements prepared, the pooler answers each `Parse` itself with a fabricated `ParseComplete`. One of those fabricated messages is written into the middle of a `RowDescription` message that is still being forwarded. The reply is then not a valid protocol stream and the client loses message framing.

With [pgx](https://github.com/jackc/pgx) the next "header" it reads is `ff ff ff 00 00`: a message of type `0xff` with length `0xffff0000`. pgx has no message size limit by default, so it tries to allocate a 4 GiB buffer. In a real service that is an out-of-memory kill, or a read that never completes.

## Run it

Needs Go 1.24 or newer and a Neon **pooled** connection string (the host contains `-pooler`).

```bash
go run . -url 'postgresql://USER:PASSWORD@ep-XXXX-pooler.REGION.aws.neon.tech/DB?sslmode=require'
```

The program uses only system catalogs and writes nothing.

1. Client connection A sends one pgx batch of 14 distinct queries, each `select * from pg_catalog.<table> limit $1`, in pgx's default mode (named prepared statements). This succeeds and leaves the statements prepared on a backend.
2. A new client connection B sends the same batch. With transaction pooling B is given the backend A just released; the program prints `pg_backend_pid()` to show it.

Flags:

| Flag | Meaning |
|---|---|
| `-url` | Connection string. `DATABASE_URL` is used if it is not given. |
| `-mode` | pgx execution mode: `cache_statement` (default, named statements), `cache_describe`, `describe_exec`, `exec`. |
| `-n` | Statements per batch. Default 14. |
| `-rounds` | How many new client connections send the batch after A. Default 3. |

The program sets a 16 MiB message limit on the pgx frontend so that the corruption is reported as an error. Without that limit pgx would attempt the 4 GiB allocation.

## Expected

Both batches succeed, as they do on the direct (non-pooled) endpoint.

## Actual

```text
server: PostgreSQL 18.7 (3190d2f) on aarch64-unknown-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit
mode: cache_statement, statements per batch: 14

connection A (backend 1038): ok
connection B1, a new client connection (backend 1038): FAILED: invalid body length: expected at most 16777216, but got 4294901756

  reply so far, by message type: 1tT1tT1tT1tT1tT
  at byte 4219 of the reply the next header is type 0xff, length 0xffff0000: not a protocol message
  the preceding RowDescription starts at byte 3589 and says it is 630 bytes long;
  a ParseComplete (31 00 00 00 04) sits inside it at byte 4185:

    00000000  13 00 00 00 16 ff ff ff  ff ff ff 00 00 69 6e 64  |.............ind|
    00000010  65 78 70 72 73 00 00 00  0a 32 00 14 00 00 00 c2  |exprs....2......|
    00000020  31 00 00 00 04 ff ff ff  ff ff ff 00 00 69 6e 64  |1............ind|
    00000030  70 72 65 64 00 00 00 0a  32 00 15 00 00 00 c2 ff  |pred....2.......|
    (dump starts at byte 4153 of the reply)

connection B2, a new client connection (backend 1038): FAILED: invalid body length: expected at most 16777216, but got 4294901756
connection B3, a new client connection (backend 1038): FAILED: invalid body length: expected at most 16777216, but got 4294901756

3 of 3 new client connections failed
```

The dump is the `RowDescription` for `select * from pg_catalog.pg_index limit $1`, at its `indexprs` column. After the column name come the table OID `00 00 0a 32`, the attribute number `00 14` and the type OID `00 00 00 c2`. The next bytes should be the type length and type modifier (`ff ff ff ff ff ff`) and the format code. A complete `ParseComplete` message, `31 00 00 00 04`, sits between the type OID and the type length.

The `RowDescription` header still gives its original length, so the client stops reading it five bytes early. The `ParseComplete` that should follow the `RowDescription` is missing, because it is the one that was written early. The next five bytes the client treats as a header are the tail of the real `RowDescription`.

Running the program a second time against the same endpoint makes connection A fail too, since the backend then already has the statements.

## When it happens

All of these are needed:

- the pooled endpoint;
- named prepared statements, with `Parse` for several of them pipelined before a single `Sync`;
- a backend that already has those statements, so the pooler replies to `Parse` itself;
- enough description output before a further `Parse`.

Measured against an already-prepared backend:

| Configuration | Result |
|---|---|
| Pooler, named statements, `-n 3`, `-n 4` or `-n 5` | ok |
| Pooler, named statements, `-n 6`, `-n 7`, `-n 8` or `-n 14` | corrupted on every attempt |
| Pooler, `-mode cache_describe` | ok |
| Pooler, `-mode describe_exec` | ok |
| Pooler, `-mode exec` | ok |
| Direct endpoint, named statements, `-n 14` | ok |

The position is consistent. Here the stray message is at byte 4,185 of what the client receives, after five fabricated `ParseComplete` messages, so 4,160 bytes into what Postgres itself sent. In the application where this was first seen, with an unrelated set of statements, it was at byte 4,210 after ten fabricated `ParseComplete` messages: again 4,160 bytes into Postgres' own output. In both cases it is the `ParseComplete` for the next statement, written while the previous statement's `RowDescription` is partly forwarded.

PgBouncer's default `pkt_buf` is 4,096 bytes, which may be the boundary involved. That is a guess: the pooler's version and configuration are not visible from the client.

## Why it looks intermittent in a real service

The first batch on a backend with nothing prepared always succeeds, because the pooler forwards the real `Parse` messages. A client connection that has already prepared the statements sends no `Parse` at all, so it keeps working too.

It fails for a client connection that has not prepared them when the backend has: a new process, or a not-yet-used connection in a client-side pool. After the Neon compute resumes from suspend every backend is new, so it works again for a while.

## Workarounds

- Do not use named prepared statements for batches through the pooler. In pgx: `default_query_exec_mode=cache_describe`, `describe_exec` or `exec` in the connection string.
- Use the direct endpoint.
- Set a maximum message size in the client so the corruption is an error and not an allocation. In pgx: `conn.PgConn().Frontend().SetMaxBodyLen(n)`.

## Environment

- Neon, region `aws-us-west-2`, PostgreSQL 18.7, pooled endpoint in transaction mode
- Go 1.25, `github.com/jackc/pgx/v5` v5.8.0
- Reproduced from Google Cloud Run (us-west1) and from a laptop

Only pgx was tested. Other drivers that send several `Parse` messages before a `Sync` may be affected.

## Licence

MIT. See [LICENSE](LICENSE).
