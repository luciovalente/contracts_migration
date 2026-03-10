# Migrazione `sorgenia.contracts` da PostgreSQL a MongoDB

Script Python per migrare dati da PostgreSQL (`sorgenia.contracts`) verso MongoDB (`sorgenia.contracts`) con regole:

- query base con alias già allineati ai campi Mongo;
- arricchimento tramite sequenza ordinata di lookup PostgreSQL;
- inserimento idempotente su Mongo (`$setOnInsert`).

## 1) Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Compila il file `.env` con le credenziali reali di PostgreSQL e MongoDB.

## 2) Organizzazione per sequenze (riordinabile)

Lo script `src/migrate_contracts.py` è organizzato in blocchi che puoi spostare facilmente:

- `BASE_QUERY_COLUMNS`: colonne query sorgente con alias target Mongo;
- `BASE_DOCUMENT_FIELDS`: campi che finiscono nel documento base;
- `PG_LOOKUP_STEPS`: sequenza di query lookup PostgreSQL;
- `MONGO_WRITE_STEPS`: sequenza di scritture Mongo.

Per cambiare ordine di esecuzione ti basta riordinare gli elementi nelle liste.

## 3) Esecuzione

```bash
python src/migrate_contracts.py
```

Per testare la migrazione su un sottoinsieme di contratti puoi impostare un filtro opzionale via variabile ambiente.

Formato atteso per `CONTRACT_NAMES_FILTER`:
- stringa CSV (valori separati da virgola);
- ogni valore deve essere il contenuto esatto del campo `name` in `sorgenia.contracts`;
- eventuali spazi prima/dopo i valori vengono ignorati.

Esempi validi:

```bash
export CONTRACT_NAMES_FILTER="CONTRACT_001,CONTRACT_002"
export CONTRACT_NAMES_FILTER="CONTRACT_001, CONTRACT_002, CONTRACT_003"
python src/migrate_contracts.py
```

Quando `CONTRACT_NAMES_FILTER` non è valorizzata (o è vuota), lo script migra tutti i record di `sorgenia.contracts`.

Lo script espone inoltre log di avanzamento in CLI (query sorgente, batch processati, riepilogo finale).

Output atteso:

- numero documenti creati;
- numero record già presenti e ignorati.

## 4) Note chiave implementative

- query di base: `SELECT <campi con alias> FROM sorgenia.contracts`;
- chiave idempotente su Mongo: `name`;
- insert-only su Mongo tramite `update_one(..., upsert=True, $setOnInsert=...)`.
