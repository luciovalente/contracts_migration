# Migrazione `sorgenia.contracts` da PostgreSQL a MongoDB

Script Python per migrare dati da PostgreSQL (`sorgenia.contracts`) verso MongoDB (`sorgenia.contracts`) con regole:

- ignora i campi marcati come cancellati (`deleted: true`);
- arricchisce i dati con lookup su altre tabelle solo se il record collegato esiste;
- crea il documento su Mongo solo se non esiste già (`$setOnInsert`).

## 1) Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Compila il file `.env` con le credenziali reali di PostgreSQL e MongoDB.

## 2) Configurazione mapping

Il file `mapping/contracts_mapping.yaml` contiene:

- `fields`: mapping colonna sorgente -> campo target Mongo;
- `deleted: true`: campo da ignorare;
- `lookups`: relazioni su altre tabelle (join logico tramite FK) da applicare solo se il record collegato esiste.

Puoi estendere il mapping in base allo spreadsheet completo.

## 3) Esecuzione

```bash
python src/migrate_contracts.py
```

Output atteso:

- numero documenti creati;
- numero record già presenti e ignorati.

## 4) Note chiave implementative

- query di base: `SELECT <campi_mappati> FROM sorgenia.contracts`;
- chiave idempotente su Mongo: `name`;
- insert-only su Mongo tramite `update_one(..., upsert=True, $setOnInsert=...)`.
