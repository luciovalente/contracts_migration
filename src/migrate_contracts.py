#!/usr/bin/env python3
"""Migrazione contratti da PostgreSQL a MongoDB.

Organizzazione a step:
1. Query base su sorgenia.contracts con alias già allineati ai campi Mongo.
2. Sequenza ordinata di lookup PostgreSQL (riordinabile in PG_LOOKUP_STEPS).
3. Sequenza ordinata di write Mongo (riordinabile in MONGO_WRITE_STEPS).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.extras
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
    contract_names_filter: list[str]


@dataclass(frozen=True)
class LookupStep:
    name: str
    source_fk: str
    schema: str
    table: str
    table_pk: str
    selected_fields: list[str]
    target_map: dict[str, str]


@dataclass(frozen=True)
class MongoWriteStep:
    name: str
    filter_field: str


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Query principale: alias -> campo target Mongo.
BASE_QUERY_COLUMNS: list[str] = [
    "name",
    "activation_date AS activationdate",
    "case_id",
    "cig_code",
    "client_id AS accountcode",
    "commodity_type",
    "contact_id AS contactid",
    "contract_end_date AS disconnectiondate",
    "contract_type AS use_type",
    "cup",
    "e_invoice",
    "institution_name",
    "ipa_code",
    "is_split_iva",
    "office_code",
    "sdi_code",
    "sdi_write_date",
    "sign_date AS signaturedate",
    "sign_location AS luogo_firma",
    # FK tecniche per i lookup successivi.
    "client_id",
    "contact_id",
    "billing_profile_id",
    "res_partner_id",
]

# Campi del documento base Mongo (escludendo FK tecniche).
BASE_DOCUMENT_FIELDS: list[str] = [
    "name",
    "activationdate",
    "case_id",
    "cig_code",
    "accountcode",
    "commodity_type",
    "contactid",
    "disconnectiondate",
    "use_type",
    "cup",
    "e_invoice",
    "institution_name",
    "ipa_code",
    "is_split_iva",
    "office_code",
    "sdi_code",
    "sdi_write_date",
    "signaturedate",
    "luogo_firma",
]

# Step di lookup: basta riordinare o aggiungere elementi qui.
PG_LOOKUP_STEPS: list[LookupStep] = [
    LookupStep(
        name="contracts_from_client_id",
        source_fk="client_id",
        schema="sorgenia",
        table="contracts",
        table_pk="id",
        selected_fields=["accountcode", "commodity_type"],
        target_map={
            "accountcode": "contracts.accountcode",
            "commodity_type": "contracts.commodity_type",
        },
    ),
    LookupStep(
        name="order_from_contact_id",
        source_fk="contact_id",
        schema="sorgenia",
        table="order",
        table_pk="id",
        selected_fields=["contactid", "luogo_sottoscrittore_conto"],
        target_map={
            "contactid": "order.contactid",
            "luogo_sottoscrittore_conto": "order.luogo_sottoscrittore_conto",
        },
    ),
    LookupStep(
        name="billing_profile_from_billing_profile_id",
        source_fk="billing_profile_id",
        schema="sorgenia",
        table="billing_profile",
        table_pk="id",
        selected_fields=[
            "cig_code",
            "cup",
            "e_invoice",
            "institution_name",
            "ipa_code",
            "office_code",
            "sdi_code",
            "sdi_write_date",
        ],
        target_map={
            "cig_code": "billing_profile.cig_code",
            "cup": "billing_profile.cup",
            "e_invoice": "billing_profile.e_invoice",
            "institution_name": "billing_profile.institution_name",
            "ipa_code": "billing_profile.ipa_code",
            "office_code": "billing_profile.office_code",
            "sdi_code": "billing_profile.sdi_code",
            "sdi_write_date": "billing_profile.sdi_write_date",
        },
    ),
    LookupStep(
        name="res_partner_from_res_partner_id",
        source_fk="res_partner_id",
        schema="sorgenia",
        table="res_partner",
        table_pk="id",
        selected_fields=["is_split_iva"],
        target_map={"is_split_iva": "res_partner.is_split_iva"},
    ),
]

# Step Mongo: oggi insert-only idempotente; puoi aggiungerne altri in sequenza.
MONGO_WRITE_STEPS: list[MongoWriteStep] = [
    MongoWriteStep(name="insert_contract_if_missing", filter_field="name")
]


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
    raw_contract_filter = os.getenv("CONTRACT_NAMES_FILTER", "")
    contract_names_filter = [name.strip() for name in raw_contract_filter.split(",") if name.strip()]

    return AppConfig(
        postgres=pg,
        mongo=mongo,
        batch_size=int(env("BATCH_SIZE", "500")),
        contract_names_filter=contract_names_filter,
    )


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


def build_base_query(schema: str, table: str, contract_names_filter: list[str]) -> tuple[str, list[Any]]:
    fields_sql = ", ".join(BASE_QUERY_COLUMNS)
    base_query = f"SELECT {fields_sql} FROM {quote_ident(schema)}.{quote_ident(table)}"

    if not contract_names_filter:
        return base_query, []

    placeholders = ", ".join(["%s"] * len(contract_names_filter))
    filtered_query = f"{base_query} WHERE {quote_ident('name')} IN ({placeholders})"
    return filtered_query, contract_names_filter


def extract_base_document(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row[field] for field in BASE_DOCUMENT_FIELDS if row.get(field) is not None}


def build_lookup_query(step: LookupStep) -> str:
    selected = ", ".join([quote_ident(step.table_pk), *(quote_ident(field) for field in step.selected_fields)])
    return (
        f"SELECT {selected} "
        f"FROM {quote_ident(step.schema)}.{quote_ident(step.table)} "
        f"WHERE {quote_ident(step.table_pk)} = %s"
    )


def apply_lookup_steps(lookup_cur, row: dict[str, Any], doc: dict[str, Any]) -> None:
    for step in PG_LOOKUP_STEPS:
        fk_value = row.get(step.source_fk)
        if fk_value is None:
            continue

        lookup_cur.execute(build_lookup_query(step), (fk_value,))
        found = lookup_cur.fetchone()
        if found is None:
            continue

        for source_col, target_col in step.target_map.items():
            value = found.get(source_col)
            if value is not None:
                doc[target_col] = value


def apply_mongo_write_steps(mongo_col: Collection, doc: dict[str, Any]) -> bool:
    for step in MONGO_WRITE_STEPS:
        unique_value = doc.get(step.filter_field)
        if unique_value is None:
            return False

        result = mongo_col.update_one(
            {step.filter_field: unique_value},
            {"$setOnInsert": doc},
            upsert=True,
        )
        if result.upserted_id is not None:
            return True

    return False


def migrate() -> None:
    logger.info("[1/5] Caricamento configurazione da variabili ambiente")
    cfg = load_config()

    if cfg.contract_names_filter:
        logger.info(
            "Filtro contratti attivo: verranno migrati solo %d contratti (%s)",
            len(cfg.contract_names_filter),
            ", ".join(cfg.contract_names_filter),
        )
    else:
        logger.info("Nessun filtro contratti attivo: verranno valutati tutti i record")

    logger.info("[2/5] Apertura connessioni a PostgreSQL e MongoDB")
    pg_conn = pg_connect(cfg.postgres)
    mongo_col = mongo_collection(cfg.mongo)

    try:
        logger.info("[3/5] Avvio lettura batch da PostgreSQL")
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur, pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as lookup_cur:
            query, query_params = build_base_query(cfg.postgres.schema, cfg.postgres.table, cfg.contract_names_filter)
            logger.info("Esecuzione query sorgente")
            cur.execute(query, query_params)

            migrated = 0
            skipped_existing = 0
            skipped_missing_name = 0
            processed = 0

            while True:
                rows = cur.fetchmany(cfg.batch_size)
                if not rows:
                    break

                logger.info("Batch ricevuto: %d record", len(rows))
                for row in rows:
                    processed += 1
                    document = extract_base_document(row)
                    if "name" not in document:
                        skipped_missing_name += 1
                        continue

                    apply_lookup_steps(lookup_cur, row, document)
                    inserted = apply_mongo_write_steps(mongo_col, document)
                    if inserted:
                        migrated += 1
                    else:
                        skipped_existing += 1

            logger.info("[4/5] Migrazione completata")
            logger.info(
                "Totale processati: %d | Creati: %d | Esistenti ignorati: %d | Senza name: %d",
                processed,
                migrated,
                skipped_existing,
                skipped_missing_name,
            )
    finally:
        logger.info("[5/5] Chiusura connessione PostgreSQL")
        pg_conn.close()


if __name__ == "__main__":
    migrate()
