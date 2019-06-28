"""
This script converts data from CHB-MIT Scalp EEG Database (https://physionet.org/pn6/chbmit/), which is encoded in the
European Data Format (.edf, https://www.edfplus.info/), into csv.

Version History:
v0.1 (June 17, 2019): first version, meant to convert a single file (see the sourceFile variable) and upload to a S3
    bucket
v0.2 (June 25, 2019): go from EDF in S3 to CSV in S3
Author:
David Lee, Insight Data Engineering Fellow, New York City 2019
"""


import pyedflib
import numpy as np
import sys
import os
import boto3


if len(sys.argv) != 4:
    sys.stderr.write('Usage: %s <subject #> <tape #> <tape type #>\n' % sys.argv[0])
    sys.exit(1)

source_bucket = "physionet-chbmit-mirror"
export_bucket = "speegs-source-chbmit"

subject_id = sys.argv[1]
replay_file_number = sys.argv[2]
tape_type = sys.argv[3]

common_key = f"chb{subject_id}_{replay_file_number}"
source_key = f"chb{subject_id}/{common_key}.edf"
export_key = f"chb{subject_id}/{common_key}.csv"
temporary_source = f"/tmp/{common_key}.edf"
temporary_export = f"/tmp/{common_key}.csv"

s3 = boto3.client('s3')
s3.download_file(source_bucket, source_key, temporary_source)

f = pyedflib.EdfReader(temporary_source)

numChannels = f.signals_in_file
assert(numChannels > 0) # there should be at least 1 channel.
sampleFrequency = f.getSampleFrequency(0)
numSamples = int(f.getNSamples()[0])

# Generate the label for time
timeLabel = np.linspace(0, numSamples / float(sampleFrequency), num=numSamples, endpoint=False)

# initialize the exporting, empty matrix
exportArray = np.empty((numSamples, numChannels + 1))
exportArray[:, 0] = timeLabel
exportHeader = "Time"

for i in range(numChannels):
    exportHeader += "," + f.getLabel(i)
    exportArray[:, i + 1] = f.readSignal(i)

exportHeader += ".1"

# Tape type
# If tape type == 1, tape has 23 channels in a pristine condition
# If tape type == 2, tape has an extraneous 24th channel that must be deleted
# If tape type == 3, tape has empty channels at: 5, 10, 13, 18, 23
if tape_type == "2":
    exportArray = np.delete(exportArray, 24, axis=1)
if tape_type == "3":
    exportArray = np.delete(exportArray, [5, 10, 13, 18, 23], axis=1)

# Save the exported csv
np.savetxt(temporary_export, exportArray, fmt='%.6f', delimiter=',', header=exportHeader)

# Upload the exported csv to S3
s3.upload_file(temporary_export, export_bucket, export_key)

os.remove(temporary_source)
#os.remove(temporary_export)

print(f"Successfully exported {common_key}.")

