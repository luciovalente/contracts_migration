#!/usr/bin/env python3
"""Migrazione contratti con sequenza esplicita di query/update.

Flusso per ogni record di `sorgenia.contracts`:
1) lettura da PostgreSQL (`sorgenia.contracts`);
2) insert/upsert su MongoDB (`contract`);
3) update MongoDB (`order`);
4) update MongoDB (`orderitems`);
5) update PostgreSQL (`sorgenia.billing_profile`);
6) update PostgreSQL (`sorgenia.res_partner`).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
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


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db: str
    contract_collection: str
    order_collection: str
    orderitems_collection: str


@dataclass(frozen=True)
class AppConfig:
    postgres: PostgresConfig
    mongo: MongoConfig
    batch_size: int
    contract_names_filter: list[str]


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


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
    )
    mongo = MongoConfig(
        uri=env("MONGO_URI"),
        db=env("MONGO_DB", "sorgenia"),
        contract_collection=env("MONGO_CONTRACT_COLLECTION", "contract"),
        order_collection=env("MONGO_ORDER_COLLECTION", "order"),
        orderitems_collection=env("MONGO_ORDERITEMS_COLLECTION", "orderitems"),
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


def mongo_collections(cfg: MongoConfig) -> tuple[Collection, Collection, Collection]:
    client = MongoClient(cfg.uri)
    db = client[cfg.db]
    return db[cfg.contract_collection], db[cfg.order_collection], db[cfg.orderitems_collection]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_contracts_query(schema: str, contract_names_filter: list[str]) -> tuple[str, list[Any]]:
    base_query = f"SELECT * FROM {quote_ident(schema)}.{quote_ident('contracts')}"
    if not contract_names_filter:
        return base_query, []

    placeholders = ", ".join(["%s"] * len(contract_names_filter))
    query = f"{base_query} WHERE {quote_ident('name')} IN ({placeholders})"
    return query, contract_names_filter


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_contract_document(row: dict[str, Any]) -> dict[str, Any]:
    now = utc_now()
    contract_id = row.get("id")
    return {
        "_id": str(contract_id) if contract_id is not None else row.get("name"),
        "accountcode": row.get("client_id"),
        "additional_documents": row.get("additional_documents"),
        "cd_proposta": row.get("proposal_code"),
        "channel": row.get("channel"),
        "code": row.get("code"),
        "commodity_type": row.get("commodity_type"),
        "contract_number": row.get("contract_number"),
        "contract_type": row.get("contract_type"),
        "contractstatus": row.get("contract_status") or "Active",
        "created_by": row.get("created_by") or "migration-script",
        "createdate": row.get("create_date") or now,
        "description": row.get("description"),
        "document_id": row.get("document_id"),
        "enddate": row.get("contract_end_date"),
        "flag_attivazione_ancitipata": row.get("flag_attivazione_ancitipata"),
        "internaldata": {
            "modified_by": row.get("updated_by") or row.get("created_by") or "migration-script",
            "_region": row.get("region") or "EU",
        },
        "name": row.get("name"),
        "note": row.get("note"),
        "sending_date": row.get("sending_date"),
        "signaturedate": row.get("sign_date"),
        "sm_name": "contract",
        "sm_reason": "Migrated from sorgenia.contracts",
        "sm_state": row.get("sm_state") or "attivo",
        "sm_version": row.get("sm_version") or 1,
        "startdate": row.get("activation_date") or now,
        "subtype": row.get("subtype"),
        "totalvolumeconsumption": row.get("totalvolumeconsumption"),
        "type": row.get("type") or "InOrder",
        "updated_by": row.get("updated_by"),
        "updatedate": row.get("write_date") or now,
    }


def insert_contract(contract_col: Collection, contract_doc: dict[str, Any]) -> None:
    contract_col.update_one(
        {"_id": contract_doc["_id"]},
        {"$setOnInsert": contract_doc},
        upsert=True,
    )


def update_order(order_col: Collection, contract_doc: dict[str, Any]) -> None:
    order_col.update_many(
        {"contract_id": contract_doc["_id"]},
        {
            "$set": {
                "accountcode": contract_doc.get("accountcode"),
                "contract_number": contract_doc.get("contract_number"),
                "contractstatus": contract_doc.get("contractstatus"),
                "updatedate": utc_now(),
            }
        },
    )


def update_orderitems(orderitems_col: Collection, contract_doc: dict[str, Any]) -> None:
    orderitems_col.update_many(
        {"contract_id": contract_doc["_id"]},
        {
            "$set": {
                "accountcode": contract_doc.get("accountcode"),
                "contract_number": contract_doc.get("contract_number"),
                "updatedate": utc_now(),
            }
        },
    )


def update_billing_profile(pg_cur, schema: str, row: dict[str, Any]) -> None:
    billing_profile_id = row.get("billing_profile_id")
    if billing_profile_id is None:
        return

    pg_cur.execute(
        f"""
        UPDATE {quote_ident(schema)}.{quote_ident('billing_profile')}
           SET cig_code = %s,
               cup = %s,
               e_invoice = %s,
               institution_name = %s,
               ipa_code = %s,
               office_code = %s,
               sdi_code = %s,
               sdi_write_date = COALESCE(%s, sdi_write_date)
         WHERE id = %s
        """,
        (
            row.get("cig_code"),
            row.get("cup"),
            row.get("e_invoice"),
            row.get("institution_name"),
            row.get("ipa_code"),
            row.get("office_code"),
            row.get("sdi_code"),
            row.get("sdi_write_date"),
            billing_profile_id,
        ),
    )


def update_res_partner(pg_cur, schema: str, row: dict[str, Any]) -> None:
    res_partner_id = row.get("res_partner_id")
    if res_partner_id is None:
        return

    pg_cur.execute(
        f"""
        UPDATE {quote_ident(schema)}.{quote_ident('res_partner')}
           SET is_split_iva = COALESCE(%s, is_split_iva)
         WHERE id = %s
        """,
        (row.get("is_split_iva"), res_partner_id),
    )


def migrate() -> None:
    logger.info("[1/6] Caricamento configurazione")
    cfg = load_config()

    logger.info("[2/6] Apertura connessioni")
    pg_conn = pg_connect(cfg.postgres)
    contract_col, order_col, orderitems_col = mongo_collections(cfg.mongo)

    query, params = build_contracts_query(cfg.postgres.schema, cfg.contract_names_filter)

    total = 0
    with pg_conn, pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as read_cur:
        logger.info("[3/6] Query sorgente: lettura da sorgenia.contracts")
        read_cur.execute(query, params)

        while True:
            rows = read_cur.fetchmany(cfg.batch_size)
            if not rows:
                break

            for row in rows:
                total += 1
                contract_doc = build_contract_document(row)
                if contract_doc.get("_id") is None:
                    continue

                insert_contract(contract_col, contract_doc)
                update_order(order_col, contract_doc)
                update_orderitems(orderitems_col, contract_doc)

                with pg_conn.cursor() as write_cur:
                    update_billing_profile(write_cur, cfg.postgres.schema, row)
                    update_res_partner(write_cur, cfg.postgres.schema, row)

    logger.info("[4/6] Migrazione completata. Record processati: %d", total)
    logger.info("[5/6] Commit PostgreSQL completato")
    pg_conn.close()
    logger.info("[6/6] Chiusura connessione PostgreSQL")


if __name__ == "__main__":
    migrate()
