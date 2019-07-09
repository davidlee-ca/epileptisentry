#!/usr/bin/env bash
#
# Deploy epileptiSentry's Kafka cluster (5 m5.large's)
#
# System requirements: Insight Pegasus (https://github.com/InsightDataScience/pegasus)
#                      AWS CLI
#
# Note: you must provide your own EC2 keyfile and add to this directory for "peg up" commands.
#

peg up kafka-master.yml
peg up kafka-workers.yml
peg fetch kafka-cluster
peg install kafka-cluster ssh
peg install kafka-cluster aws
peg install kafka-cluster environment
peg install kafka-cluster zookeeper
peg service kafka-cluster zookeeper start
peg install kafka-cluster kafka
peg service kafka-cluster kafka start