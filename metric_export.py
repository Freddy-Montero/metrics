#!/usr/bin/env python3

from datetime import datetime, timedelta, timezone
from logging import config as logging_config

import csv
import json
import logging
import requests
import urllib3
import yaml
import sys

conf_dict = {}

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(module)s %(message)s'
            },
        },
    'handlers': {
        'stdout': {
            'class': 'logging.StreamHandler',
            'stream': sys.stdout,
            'formatter': 'verbose',
            },
        },
    'loggers': {
        'metrics': {
            'handlers': ['stdout'],
            'level': logging.INFO,
            'propagate': True,
            },
        }
    }

logging_config.dictConfig(LOGGING)


logger = logging.getLogger("metrics")

with open('/conf/config.yaml') as config_file:
    conf_dict = yaml.safe_load(config_file)

with open('/var/run/secrets/kubernetes.io/serviceaccount/token') as token_file:
    conf_dict['svc_token'] = token_file.read()

def query_prometheus(query, start_time, end_time, step):
    headers = {}
    headers['Authorization'] = 'Bearer {0}'.format(conf_dict.get('svc_token'))
    headers['Accept'] = 'application/json'
    url = conf_dict.get('prometheus_query_url', 'https://prometheus-k8s.openshift-monitoring.svc.cluster.local:9091/api/v1/query_range')
    verify_tls = conf_dict.get('verify_tls', True)
    params = {'query': query, 'start': start_time.timestamp(), 'end': end_time.timestamp(), 'step': step}
    resp = requests.post(url, headers=headers, data=params, verify=verify_tls)
    logger.debug(resp.text)
    try:
        return resp.json()
    except Exception:
        logger.error('Unable to process prometheus data: {0}'.format(resp.text))
        return {'data':{'result':[]}}

def handle_report(start_time, end_time, step):
    output_dict = {}
    for metric, query in conf_dict.get("query_map",{}).items():
        result = query_prometheus(query, start_time, end_time, step)
        if len(result['data']['result']) == 0:
            continue
        data = result['data']['result'][0]['values']
        for row in data:
            if row[0] in output_dict:
                output_dict[row[0]][metric] = row[1]
            else:
                output_dict[row[0]] = {metric: row[1]}
    output_rows = []
    for timestamp, data in output_dict.items():
        row_dict = {}
        row_dict["ts"] = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
        for key, value in data.items():
            row_dict[key] = round(float(value), 3)
        output_rows.append(row_dict)
    for row in output_rows:
        logger.info("{0}\n".format(json.dumps(row)))

if __name__ == "__main__":
    if not conf_dict.get("verify_tls", True):
        urllib3.disable_warnings()
    start_time = datetime.now(tz=timezone.utc) - timedelta(hours=conf_dict.get("cron_time", 1))
    end_time = datetime.now(tz=timezone.utc)
    step = conf_dict.get('step_time', '5m')
    handle_report(start_time, end_time, step)
