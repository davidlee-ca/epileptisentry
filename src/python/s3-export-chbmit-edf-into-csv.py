"""
This script converts electroencephalogram (EEG) data from the CHB-MIT Scalp EEG Database [1], 
which is encoded in the European Data Format (.edf) [2], into csv.

Author:
David Lee, Insight Data Engineering Fellow, New York City 2019

Reference:
[1] https://physionet.org/pn6/chbmit/
[2] https://www.edfplus.info/
"""


import pyedflib
import numpy as np
import sys
import os
import boto3


# Constants
source_bucket = "physionet-chbmit-mirror"
export_bucket = "speegs-source-chbmit"


if __name__ == "__main__":

    # Extract expected arguments

    if len(sys.argv) != 4:
        sys.stderr.write('Usage: %s <subject #> <tape #> <tape type #>\n' % sys.argv[0])
        sys.exit(1)

    subject_id = sys.argv[1]
    replay_file_number = sys.argv[2]

    # Source EDF files have varied channel setup (e.g. blank channel to separate channel groups for readability).
    # This user-provided argument will enable the logic to standardize the csv output.
    tape_type = sys.argv[3] 
 
    common_key = f"chb{subject_id}_{replay_file_number}"
    source_key = f"chb{subject_id}/{common_key}.edf"
    export_key = f"chb{subject_id}/{common_key}.csv"
    temporary_source = f"/tmp/{common_key}.edf"
    temporary_export = f"/tmp/{common_key}.csv"

    # Download the .edf file from S3
    s3 = boto3.client('s3')
    s3.download_file(source_bucket, source_key, temporary_source)
    edf_file = pyedflib.EdfReader(temporary_source)

    # Read in metadata from the EDF file
    number_of_channels = edf_file.signals_in_file
    assert(number_of_channels > 0) # there should be at least 1 channel.
    sample_frequency = edf_file.getSampleFrequency(0)
    number_of_rows = int(edf_file.getNSamples()[0])

    # Generate the label for time
    time_label = np.linspace(0, number_of_rows / float(sample_frequency), num=number_of_rows, endpoint=False)

    # initialize the exporting, empty matrix
    export_matrix = np.empty((number_of_rows, number_of_channels + 1))
    export_matrix[:, 0] = time_label
    export_header = "Time"

    # populate the output matrix from the source file
    for ch in range(number_of_channels):
        export_header += "," + edf_file.getLabel(ch)
        export_matrix[:, ch + 1] = edf_file.readSignal(ch)

    export_header += ".1"

    # "Tape type"
    # If tape type == 1, tape has 23 channels in a pristine condition
    # If tape type == 2, tape has an extraneous 24th channel that must be deleted
    # If tape type == 3, tape has empty channels at: 5, 10, 13, 18, 23
    # If tape type == 4, tape has a bunch of reference channels -- chb15 only
    if tape_type == "2":
        export_matrix = np.delete(export_matrix, 24, axis=1)
    if tape_type == "3":
        export_matrix = np.delete(export_matrix, [5, 10, 13, 18, 23], axis=1)
    if tape_type == "4":
        export_matrix = np.delete(export_matrix, [5, 10, 13, 14, 19, 24, 30, 31, 32, 33, 34, 35, 36, 37, 38], axis=1)


    # Save the exported csv
    np.savetxt(temporary_export, export_matrix, fmt='%.6f', delimiter=',', header=export_header)

    # Upload the exported csv to S3
    s3.upload_file(temporary_export, export_bucket, export_key)

    os.remove(temporary_source)
    # os.remove(temporary_export)

    print(f"s3-export-chbmit-edf-into-csv: successfully exported {common_key}.")
