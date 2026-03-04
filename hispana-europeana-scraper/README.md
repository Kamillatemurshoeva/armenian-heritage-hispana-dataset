# Hispana (Spain) → Europeana Metadata Extractor (Armenian Queries)

Hispana is Spain's national portal for digital cultural heritage and the national aggregator to Europeana:
- https://hispana.mcu.es

This repository extracts metadata for records **provided by Hispana** via the **Europeana Search API** (open API access), using a provider filter:

`qf = PROVIDER:"Hispana"`

It supports multiple search terms (comma-separated) and produces:
- Raw dataset (CSV)
- Cleaned dataset (CSV)
- Cleaned dataset (JSONL)

Default query terms:
- `armenio, armenia, armenios, armenias`

## Open data note
This project **does not redistribute images or full digital objects**. It collects **metadata and links** that point back to the original providers.
Please follow **Hispana/Europeana terms of use** and any rights statements attached to individual records when reusing content.

## Data Source
- **Name:** Hispana
- **URL:** https://hispana.mcu.es
- **Type:** National Digital Heritage Aggregator (Spain)

## Fields Extracted
If available, the script exports:
- `title`
- `date`
- `creator`
- `description`
- `original_url` (link to the original object)
- `europeana_link`
- `id`
- `type`
- `provider`
- `data_provider`
- `country`
- `language`

Additional cleaned columns:
- `armenian_match_field` (title / description / title+description)
- `armenian_relevance` (heuristic: direct vs incidental)

## Setup

### 1) Create a virtual environment (recommended)
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Set your Europeana API key
Get a key from Europeana and set it as an environment variable:

```bash
export EUROPEANA_API_KEY="YOUR_KEY_HERE"
```

## Usage

### Default run (writes to ./data)
```bash
python extract_hispana_europeana.py
```

### Custom query
```bash
python extract_hispana_europeana.py -q "armenio, armenia"
```

### Save to a custom folder
```bash
python extract_hispana_europeana.py -o output
```

### Disable Armenian filtering
```bash
python extract_hispana_europeana.py --no-filter
```

### Keep only "direct" matches
```bash
python extract_hispana_europeana.py --only-direct
```

## Output files
Files are saved under the output directory (default: `data/`), e.g.:

- `hispana_europeana_armenio_raw.csv`
- `hispana_europeana_armenio_clean.csv`
- `hispana_europeana_armenio_clean.jsonl`

Paths are printed at the end of the run.

## Method (why Europeana API?)
Hispana is an aggregator to Europeana, so querying Europeana with `PROVIDER:"Hispana"` is a robust, stable way to collect Hispana-provided records without brittle HTML scraping.

## License
MIT
