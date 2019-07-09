#!/usr/bin/env bash
#
# Install Python 3.7.3 and set up the project's virtual environment.
# At the time of the Insight Data Engineering Fellowship Program, the latest official Python version was 3.6.8.
#

sudo apt-get install -y build-essential libssl-dev libffi-dev python3-dev python3-pip python3-venv librocksdb-dev libsnappy-dev libbz2-dev zlib1g-dev liblz4-dev libpq-dev openjdk-8-jdk awscli

sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update -y
sudo apt-get install -y python3.7 python3.7-dev python3.7-venv

python3.7 -m venv insightenv
source insightenv/bin/activate

python -m pip install --upgrade pip
python -m pip install boto3 confluent_kafka pywavelets numpy pandas scipy

# entroPy is a Python library to calculate approximate entropy of a signal.
# At the time of the Insight Data Engineering Fellowship Program, this was not available in PyPI.

cd ~/insightenv/lib
git clone https://github.com/raphaelvallat/entropy.git
cd entropy/
python -m pip install -r requirements.txt
python setup.py develop
