# ClassicModels Local Data Pipeline

Layers implemented so far: **Source (MySQL) -> Raw -> Archive -> Curated (+Quarantine)**

## Folder structure
- `config/secrets.yaml` - DB credentials, Spark config, all local paths
- `metadata/table_config.json` - static per-table rules (load_type, watermark_column, primary_key, scd_type)
- `metadata/watermark_state.json` - dynamic incremental-load pointer per table (updated only on success)
- `metadata/schema_registry.json` - last-known schema per table, for schema-evolution detection
- `src/utils/logger.py` - structured JSON logger (source, who, activity, status, duration)
- `src/utils/metadata_manager.py` - all reads/writes to the JSON metadata store
- `src/utils/config_loader.py` - loads secrets.yaml + table_config.json
- `src/extract/source_to_raw.py` - MySQL -> Raw + Archive
- `src/transform/raw_to_curated.py` - Raw -> Curated + Quarantine
- `run_pipeline.py` - local runner, chains both stages
- `logs/pipeline.jsonl` - structured JSON logs (one line per activity event)
- `logs/pipeline_run_log.jsonl` - one line per table per layer per run (append-only)

## Run locally
```bash
pip install -r requirements.txt
# update config/secrets.yaml with your local MySQL password
python run_pipeline.py
```

## Unit tests
```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```
46 tests covering:
- `test_config_loader.py` - yaml/json config reads, active-table filtering
- `test_logger.py` - every JSON log line has source/timestamp/who/status/duration_seconds
- `test_metadata_manager.py` - watermark read/write, and specifically that a FAILED run
  never moves the watermark forward (the self-healing guarantee)
- `test_source_to_raw.py` - full vs incremental vs incremental+lookback query building,
  and schema evolution detection (new/dropped/changed columns, always non-fatal)
- `test_raw_to_curated.py` - quality-rule split (valid/quarantine) on single and composite
  primary keys, dedup behaviour, latest-raw-partition discovery

No test touches a real MySQL server or the real project config/metadata files - JDBC/query
logic is tested as pure functions, quality-rule logic runs against real local[1] Spark
DataFrames (via `tests/conftest.py`), and all file I/O uses pytest's `tmp_path`.

## Design principles applied
- Metadata-driven: load type / watermark / SCD type all come from table_config.json, never hardcoded in code
- Self-healing: watermark advances ONLY on success; a failed run is automatically retried from the same point next run, no manual reset
- Schema evolution never breaks the pipeline: drift is logged + registered, not fatal
- Bad data never breaks the pipeline: rows failing quality checks go to quarantine, valid rows still flow through
- SCD Type 2 marked in metadata for products/employees/customers - actual SCD2 merge logic belongs to the next stage (Curated -> Gold), not this one



Table	        PK Type	                                      SCD	Reasoning
customers	    Single (customerNumber)	                      Type 2	creditLimit/sales_rep history matters
employees	    Single (employeeNumber)	                      Type 2	job_title/office history matters
offices	      Single (officeCode)	                          Type 1	Address rarely changes, no analytical dependency
products	    Single (productCode)	                        Type 2	buyPrice/MSRP history critical for margin
productlines	Single (productLine)	                        Type 1	Pure descriptive text, no analytical dependency
orders	      Single (orderNumber)	                        na	Fact-grain data, not a dimension
orderdetails	Composite (orderNumber+productCode)	          na	Fact-grain data, not a dimension
payments	C   omposite (customerNumber+checkNumber)         na	Fact-grain data, not a dimension