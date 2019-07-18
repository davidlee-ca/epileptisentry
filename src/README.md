# epleptiSentry source files

## Table of Contents

1. [Signal Generation](signal-generation)
2. [Stream Processing](stream-processing)
3. [Miscellaneous](miscellaneous)

## Signal Generation

`process_signals.py` is a script that emulates an electroencephalogram (EEG) machine set up to interface with epileptiSentry. It reads the EEG data of a single subject (see [Data source](../README.md#data-source) of the main README) and produces messages into a single Kafka topic named `eeg-signal`. Each message is encoded in JSON format with  the following data structure:

Key | Value
--- | -----
Subject ID (e.g. 'chb01')<br>Channel (e.g. 'CZ-PZ') |Timestamp, in Unix-style time<br>Reading, in μV

At regular intervals, the script checks slippage of time (i.e. difference between the instrument timestamp and the elapsed time since the start of the program) and adjusts its pace of signal reproduction.

`start_producers.sh` launches 10 simultaneous processes of the above script.

## Stream Processing

`v3-calculate-indicators.py` contains PySpark code to calculate the abnormal activity indicator. To start this script, run the `start_sparkjob.sh` script to provide the necessary drivers and packages (Kafka consumer, JDBC driver for TimescaleDB).

## Miscellaneous

's3-export-chbmit-edf-into-csv.py` is a script to unpack CHB-MIT's scalp EEG data, encoded in European Data Format (EDF), into comma-delimited values (CSV) format.
