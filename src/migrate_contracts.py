#!/usr/bin/env python3
"""Migrazione contratti da PostgreSQL a MongoDB.

Regole implementate:
1. Legge da sorgenia.contracts.
2. Ignora i campi marcati come "deleted" nel file di mapping.
3. Arricchisce i dati con lookup su altre tabelle PostgreSQL, ma solo se il record correlato esiste.
4. Crea documenti in MongoDB (sorgenia.contracts) solo se non esistono già.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.extras
import yaml
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str
    schema: str
    table: str


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db: str
    collection: str


@dataclass(frozen=True)
class AppConfig:
    postgres: PostgresConfig
    mongo: MongoConfig
    batch_size: int
    mapping_file: str


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required env var: {name}")
    return value


def load_config() -> AppConfig:
    load_dotenv()
    pg = PostgresConfig(
        host=env("POSTGRES_HOST"),
        port=int(env("POSTGRES_PORT", "5432")),
        db=env("POSTGRES_DB"),
        user=env("POSTGRES_USER"),
        password=env("POSTGRES_PASSWORD"),
        schema=env("POSTGRES_SCHEMA", "sorgenia"),
        table=env("POSTGRES_TABLE", "contracts"),
    )
    mongo = MongoConfig(
        uri=env("MONGO_URI"),
        db=env("MONGO_DB", "sorgenia"),
        collection=env("MONGO_COLLECTION", "contracts"),
    )
    return AppConfig(
        postgres=pg,
        mongo=mongo,
        batch_size=int(env("BATCH_SIZE", "500")),
        mapping_file=env("MAPPING_FILE", "./mapping/contracts_mapping.yaml"),
    )


def load_mapping(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        mapping = yaml.safe_load(f)
    if not isinstance(mapping, dict):
        raise ValueError("Invalid mapping file: expected object")
    return mapping


def pg_connect(cfg: PostgresConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.db,
        user=cfg.user,
        password=cfg.password,
    )


def mongo_collection(cfg: MongoConfig) -> Collection:
    client = MongoClient(cfg.uri)
    return client[cfg.db][cfg.collection]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_select_query(schema: str, table: str, source_fields: list[str]) -> str:
    fields_sql = ", ".join(quote_ident(f) for f in source_fields)
    return f"SELECT {fields_sql} FROM {quote_ident(schema)}.{quote_ident(table)}"


def extract_base_document(row: dict[str, Any], field_rules: list[dict[str, Any]]) -> dict[str, Any]:
    doc: dict[str, Any] = {}
    for rule in field_rules:
        if rule.get("deleted", False):
            continue
        source = rule["source"]
        target = rule.get("target", source)
        value = row.get(source)
        if value is not None:
            doc[target] = value
    return doc


def add_lookup_data(lookup_cur, row: dict[str, Any], doc: dict[str, Any], lookups: list[dict[str, Any]]) -> None:
    """Esegue lookup su altre tabelle e arricchisce il documento solo con record esistenti."""
    for lookup in lookups:
        source_fk = lookup["source_fk"]
        fk_value = row.get(source_fk)
        if fk_value is None:
            continue

        schema = lookup.get("schema", "sorgenia")
        table = lookup["table"]
        table_pk = lookup["table_pk"]
        target_prefix = lookup["target_prefix"]
        field_map = lookup.get("field_map", {})
        if not field_map:
            continue

        select_fields = [table_pk, *field_map.keys()]
        select_sql = ", ".join(quote_ident(c) for c in select_fields)
        query = (
            f"SELECT {select_sql} FROM {quote_ident(schema)}.{quote_ident(table)} "
            f"WHERE {quote_ident(table_pk)} = %s"
        )
        lookup_cur.execute(query, (fk_value,))
        found = lookup_cur.fetchone()

        # Regola: arricchire solo se il record esiste.
        if found is None:
            continue

        for source_col, target_col in field_map.items():
            value = found.get(source_col)
            if value is not None:
                doc[f"{target_prefix}.{target_col}"] = value


def migrate() -> None:
    cfg = load_config()
    mapping = load_mapping(cfg.mapping_file)
    field_rules: list[dict[str, Any]] = mapping.get("fields", [])
    lookups: list[dict[str, Any]] = mapping.get("lookups", [])

    if not field_rules:
        raise ValueError("Mapping must contain at least one field in 'fields'")

    source_fields = sorted({rule["source"] for rule in field_rules if "source" in rule})

    pg_conn = pg_connect(cfg.postgres)
    mongo_col = mongo_collection(cfg.mongo)

    try:
        # Cursor dict per nome colonna.
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur, pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as lookup_cur:
            query = build_select_query(
                cfg.postgres.schema,
                cfg.postgres.table,
                source_fields,
            )
            cur.execute(query)

            migrated = 0
            skipped_existing = 0
            while True:
                rows = cur.fetchmany(cfg.batch_size)
                if not rows:
                    break

                for row in rows:
                    document = extract_base_document(row, field_rules)
                    if "name" not in document:
                        # Chiave minima per identificare il contratto.
                        continue

                    add_lookup_data(lookup_cur, row, document, lookups)

                    # Crea documento solo se non esistente.
                    result = mongo_col.update_one(
                        {"name": document["name"]},
                        {"$setOnInsert": document},
                        upsert=True,
                    )
                    if result.upserted_id is not None:
                        migrated += 1
                    else:
                        skipped_existing += 1

            print(f"Migrazione completata. Creati: {migrated}, Esistenti ignorati: {skipped_existing}")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    migrate()
