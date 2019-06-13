import datetime as dt
import time as tt
import boto3
import pandas as pd

s3 = boto3.client('s3')
obj = s3.get_object(Bucket='test-physionet-chbmit-1', Key='minimum-viable/mvp-data.csv')

ptid = "pt_mvp"

df_iter = pd.read_csv(obj['Body'], header=0, chunksize=1000) # tunable later -- chunk size for streaming and IOPS
c = 0 # for testing

for df_chunk in df_iter:
    for row in df_chunk.itertuples(index=False):
        for channel in range(1, 24):

            # for testing: print the patient id, EEG channel number, time (column 0),
            # potential reading, and the current time
            print(tuple([ptid, channel, row[0], row[channel],dt.datetime.now()]))

            # TODO: write this to Kafka
            # create patient reading channel topic
            # write into the producer reading with channel # as key

        tt.sleep(0.003) # simulates 200Hz... good enough?

    # for testing: equivalent of head(20)
    c += 1
    if c > 20:
        break

