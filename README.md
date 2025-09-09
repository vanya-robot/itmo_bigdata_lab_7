# ЛР7: Data Mart (Scala) + Model (Python) on HDFS

Architecture:

- DataMart (Scala): reads raw CSV from HDFS, preprocesses (negative->NULL, impute mean, assemble, scale) and writes parquet to HDFS (/data/mart/processed.parquet).
- Model (Python): reads preprocessed parquet from HDFS, loads the saved KMeans model, runs inference, and writes predictions to HDFS (/data/predictions/preds.parquet).

This repo contains:

- `src/datamart/DataMart.scala` — Scala Spark job that creates the data mart.
- `src/run_model.py` — Python Spark job that loads preprocessed data and runs the model.
- `src/sql/source_data.csv` — sample raw CSV to upload to HDFS.
- `Makefile` — helper commands: `upload-raw`, `run-datamart`, `run-model`, `check-preds`.
- `docker-compose-hdfs.yml` — minimal HDFS + Spark services for local testing (adjust as needed).

Quick usage (when you have a working HDFS and Spark available):

Upload raw CSV to HDFS:

```bash
```markdown
# ЛР7: Data Mart (Scala) + Model on HDFS

Architecture:

- DataMart (Scala): reads raw CSV from HDFS, preprocesses (negative->NULL, impute mean, assemble, scale) and then calls the saved Spark KMeansModel in-memory to produce predictions (no intermediate files).

This folder contains:

- `src/datamart/DataMart.scala` — Scala Spark job that reads raw CSV from HDFS, preprocesses it and runs the saved KMeansModel in the same job, printing predictions to stdout.
- `src/sql/source_data.csv` — sample raw CSV to upload to HDFS.
- `Makefile` — helper commands: `upload-raw`, `run-pipeline`, `check-preds`.
- `docker-compose-hdfs.yml` — minimal HDFS + Spark services for local testing (adjust as needed).

Quick usage (when you have a working HDFS and Spark available):

1. Upload raw CSV to HDFS:

```bash
make upload-raw
```

2. Run the pipeline (Scala datamart will preprocess and call the KMeans model in-memory):

```bash
make run-pipeline
```

Notes:
- The Scala job reads the environment variable `MODEL_PATH` (if set) to locate the saved Spark KMeansModel; otherwise edit the source to point at your model location.
- The job prints predictions to stdout; `check-preds` simply reminds you to rerun `make run-pipeline` to see results.

```markdown
