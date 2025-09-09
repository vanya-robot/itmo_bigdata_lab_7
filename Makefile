HDFS= hdfs
SPARK_SHELL=spark-shell
SPARK_SUBMIT=spark-submit

.PHONY: upload-raw run-datamart run-model check-preds

upload-raw:
	@echo "Uploading raw CSV to HDFS"
	$(HDFS) dfs -mkdir -p /data/raw || true
	$(HDFS) dfs -put -f src/sql/source_data.csv /data/raw/source_data.csv

run-datamart:
	@echo "Running DataMart (Scala) via spark-shell"
	$(SPARK_SHELL) --master local[2] -i src/datamart/DataMart.scala

run-pipeline:
	@echo "Running full pipeline (DataMart -> Model) via spark-shell"
	$(SPARK_SHELL) --master local[2] -i src/datamart/DataMart.scala

run-model:
	@echo "Running model job (Python)"
	$(SPARK_SUBMIT) --master local[2] src/run_model.py

check-preds:
	@echo "Predictions are printed by the Scala job; rerun run-pipeline to see them"
