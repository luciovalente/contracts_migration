# Migrazione `sorgenia.contracts` verso MongoDB + aggiornamenti correlati

Script Python per eseguire una sequenza esplicita di operazioni:

1. Query di lettura da PostgreSQL su `POSTGRES_SCHEMA.POSTGRES_TABLE` (default `sorgenia.contracts`).
2. Insert/upsert su MongoDB nella collection `contract` con i campi principali del documento contratto.
3. Update su MongoDB nella collection `order`.
4. Update su MongoDB nella collection `orderitems`.
5. Update su PostgreSQL su `sorgenia.billing_profile`.
6. Update su PostgreSQL su `sorgenia.res_partner`.

## 1) Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Compila `.env` con credenziali reali.

## 2) Variabili principali

- PostgreSQL:
  - `POSTGRES_HOST`
  - `POSTGRES_PORT`
  - `POSTGRES_DB`
  - `POSTGRES_USER`
  - `POSTGRES_PASSWORD`
  - `POSTGRES_SCHEMA` (default: `sorgenia`)
  - `POSTGRES_TABLE` (default: `contracts`)
- MongoDB:
  - `MONGO_URI`
  - `MONGO_DB` (default: `sorgenia`)
  - `MONGO_CONTRACT_COLLECTION` (default: `contract`)
  - `MONGO_ORDER_COLLECTION` (default: `order`)
  - `MONGO_ORDERITEMS_COLLECTION` (default: `orderitems`)
  - `MONGO_TLS_INSECURE` (default: `false`; abilita certificati TLS non validi se `true`)
- Batch:
  - `BATCH_SIZE` (default: `500`)
- Filtro opzionale contratti:
  - `CONTRACT_NAMES_FILTER` (CSV di `name`)

## 3) Esecuzione

```bash
python src/migrate_contracts.py
```

## 4) Nota sul documento `contract`

La funzione `build_contract_document` costruisce il documento Mongo con i campi richiesti (es. `_id`, `accountcode`, `cd_proposta`, `contract_number`, `contractstatus`, `signaturedate`, `startdate`, `updatedate`, `internaldata`, ecc.), valorizzando i dati da `sorgenia.contracts` e usando default dove necessario.
