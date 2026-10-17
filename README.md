# skob

Blob database with storage separate from compute. One `skob` binary serves reads, writes, or both.

```
project
  collection
    object     JSON. Indexed fields may be absent.
```

Writers use Raft only to decide which node owns a write segment. Object bytes go straight to blob storage. Indexes are Tantivy segments: local disk is a cache, the blob store is durability, and index changes rebuild in the background. Blob access goes through `skob_core::BlobStore`. S3, including RustFS, is the first adapter.

## Run

```bash
make run
```

`make help` lists the other targets: reader, writer, S3, tests, Compose, and `psql`. The same process is `cargo run -p skob-server -- serve`.

Defaults: role `all`, HTTP `0.0.0.0:8080`, PostgreSQL `0.0.0.0:5432`, filesystem blobs under `./data`. Override with the `SKOB_*` variables in `.env.example`. `--role` is `all`, `reader`, or `writer`.

```bash
curl -s localhost:8080/health
curl -s -X POST localhost:8080/v1/projects \
  -H 'content-type: application/json' \
  -d '{"id":"p1","name":"Demo"}'
```

## PostgreSQL

The database name is the project id. A table is a collection. Columns other than `id`, `revision`, and `body` are JSON paths. Unquoted names are lowercased. No password.

```bash
psql "host=127.0.0.1 port=5432 user=skob dbname=p1 sslmode=disable"
```

```sql
INSERT INTO docs (id, title, "user.address.city") VALUES ('obj1', 'harbor', 'portland');
UPDATE docs SET title = 'pier' WHERE id = 'obj1';
SELECT id, title, "user.address.city" FROM docs WHERE id = 'obj1';
```

The collection must already exist. `WHERE` is `=` predicates combined with `AND`. `--pg-disable` turns the listener off.

## Docker

```bash
docker compose up --build
```

RustFS is the S3 layer (`rustfsadmin` / `rustfsadmin`, ports 9000 and 9001). skob listens on 8080 and 5432 and creates the `skob` bucket on startup.
