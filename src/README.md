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

`start_producers.sh` launches 12 simultaneous processes of the above script.

## Stream Processing



## Miscellaneous

foobar
