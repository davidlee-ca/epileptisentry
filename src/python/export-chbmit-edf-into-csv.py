"""
This script converts data from CHB-MIT Scalp EEG Database (https://physionet.org/pn6/chbmit/), which is encoded in the
European Data Format (.edf, https://www.edfplus.info/), into csv.

Version History:
v0.1 (June 17, 2019): first version, meant to convert a single file (see the sourceFile variable) and upload to a S3
    bucket
v0.2 (June 25, 2019):
Author:
David Lee, Insight Data Engineering Fellow, New York City 2019
"""


import pyedflib
import numpy as np
import os
import boto3

basePathName = os.environ['HOME'] + "/dev"
sourcePathName = basePathName + "/chbmit/"
exportPathName = basePathName + "/chbmit_csv/"

sourceFile = "chb01/chb01_03"

sourceFileName = sourcePathName + sourceFile + ".edf"
exportFileName = exportPathName + sourceFile + ".csv"
exportS3FileKey = sourceFile + sourceFile + ".csv"

f = pyedflib.EdfReader(sourceFileName)

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

# Save the exported csv
os.makedirs(os.path.dirname(exportFileName), exist_ok=True)
np.savetxt(exportFileName, exportArray, fmt='%.6f', delimiter=',', header=exportHeader)

# Upload the exported csv to S3
s3 = boto3.client('s3')
s3.upload_file(exportFileName, 'speegs-source-chbmit', f"{sourceFile}.csv")
