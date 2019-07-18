#!/bin/bash

source ~/insightenv/bin/activate
python ~/epileptisentry/src/sig_gen/produce-signals.py chb01 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb02 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb03 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb04 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb05 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb06 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb07 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb08 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb09 &
python ~/epileptisentry/src/sig_gen/produce-signals.py chb10 &
