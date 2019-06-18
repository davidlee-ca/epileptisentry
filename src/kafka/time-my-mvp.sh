touch temp_log.txt
echo 'Starting time' >> temp_log.txt
date >> temp_log.txt
python mvp-produce-signals.py
echo 'Finished time' >> temp_log.txt
date >> temp_log.txt
