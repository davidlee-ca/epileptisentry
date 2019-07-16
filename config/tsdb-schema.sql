/*

TimescaleDB schema to capture the analyzed data.
To install TimescaleDB, follow the instructions on https://docs.timescale.com/v1.3/getting-started/installation.
To deploy this

sudo psql -U postgres -d [database-name] -a -f tsdb-schema.sql

*/

DROP TABLE IF EXISTS "eeg_analysis";
CREATE TABLE "eeg_analysis"(
    instr_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	ingest_time TIMESTAMP WITHOUT TIME ZONE,
	subject_id TEXT NOT NULL,
	channel TEXT NOT NULL,
	abnormality_indicator REAL,
	num_datapoints INTEGER,
);

SELECT create_hypertable('eeg_analysis', 'instr_time');
CREATE INDEX on eeg_analysis (subject_id, instr_time DESC);
CREATE INDEX on eeg_analysis (instr_time DESC, subject_id);
CREATE INDEX on eeg_analysis (instr_time DESC, seizure_metric);
