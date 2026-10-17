.PHONY: help run run-reader run-writer run-s3 build test up down logs rustfs psql

SKOB_DB ?= p1

help:
	@printf '%s\n' \
		'run          serve locally with filesystem blobs' \
		'run-reader   serve the read plane only' \
		'run-writer   serve the write plane on :8081' \
		'run-s3       serve locally against RustFS on :9000' \
		'build        build the skob binary' \
		'test         run crate tests' \
		'up           docker compose up --build' \
		'down         docker compose down' \
		'logs         follow the skob compose logs' \
		'rustfs       start only the S3-compatible store' \
		'psql         connect to the local PostgreSQL listener'

run:
	cargo run -p skob-server -- serve

run-reader:
	cargo run -p skob-server -- serve --role reader --pg-disable

run-writer:
	cargo run -p skob-server -- serve --role writer --bind 0.0.0.0:8081 --pg-bind 0.0.0.0:5433

run-s3:
	SKOB_BLOB=s3 \
	SKOB_S3_ENDPOINT=http://127.0.0.1:9000 \
	SKOB_S3_BUCKET=skob \
	SKOB_S3_REGION=us-east-1 \
	SKOB_S3_ACCESS_KEY=rustfsadmin \
	SKOB_S3_SECRET_KEY=rustfsadmin \
	SKOB_S3_PATH_STYLE=true \
	cargo run -p skob-server -- serve

build:
	cargo build -p skob-server

test:
	cargo test -p skob-core -p skob-storage -p skob-index -p skob-raft -p skob-server

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f skob

rustfs:
	docker compose up rustfs

psql:
	psql "host=127.0.0.1 port=5432 user=skob dbname=$(SKOB_DB) sslmode=disable"
