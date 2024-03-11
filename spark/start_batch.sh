
declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

echo "container started"
touch /var/log/cron.log
crontab ./spark/cron_batch
chmod +x ./spark/batch_submit
cron && tail -f /var/log/cron.log

# spark-submit --properties-file ./spark/conf/spark-batch.conf \
#              ./spark/batch.py 2>&1 | sed 's/^/| BATCH | /g'