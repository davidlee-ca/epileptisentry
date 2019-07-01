DROP EXTENSION IF EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb;
DROP TABLE IF EXISTS "eeg_data";

CREATE TABLE IF NOT EXISTS "eeg_data"(
    instrument_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    subject_id TEXT,
    channel TEXT,
    reading REAL
);

SELECT create_hypertable('eeg_data', 'instrument_time');
CREATE INDEX ON eeg_data (subject_id, instrument_time DESC);
CREATE INDEX ON eeg_data (instrument_time DESC, subject_id);
CREATE INDEX ON eeg_data (channel, instrument_time DESC);


DROP TABLE IF EXISTS "eeg_analysis";
CREATE TABLE "eeg_analysis"(
    instrument_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	subject_id TEXT,
	channel TEXT,
	seizure_metric NUMERIC,
	num_datapoints NUMERIC,
);

SELECT create_hypertable('eeg_analysis', 'instrument_time');
CREATE INDEX on eeg_analysis (subject_id, instrument_time DESC);
CREATE INDEX on eeg_analysis (instrument_time DESC, subject_id);
CREATE INDEX on eeg_analysis (instrument_time DESC, seizure_metric);
