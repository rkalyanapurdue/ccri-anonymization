#!/bin/env python3

import redis
import csv

r = redis.StrictRedis(host='localhost', port=6379, db=0)

with open('/data/job_ts_metrics_nov.csv','r') as srcfile:
    reader = csv.DictReader(srcfile)
    fieldnames = reader.fieldnames

    with open('/data/job_ts_metrics_nov_anon.csv','w') as resfile:
        writer = csv.DictWriter(resfile,fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:

            host = row['Host']
            host_key = 'HOST_%s' % host

            if not r.exists(host_key):
                r.incr('node')
                anon_val = '%s%s' % ('NODE',r.get('node').decode('utf-8'))
                r.set(host_key,anon_val)

            anon_host = r.get(host_key).decode('utf-8')

            row['Host'] = anon_host

            job_id = row['Job Id']
            jobid_key = 'JOB_%s' % job_id

            if not r.exists(jobid_key):
                r.incr('job')
                anon_val = '%s%s' % ('JOB',r.get('job').decode('utf-8'))
                r.set(jobid_key,anon_val)

            anon_jobid = r.get(jobid_key).decode('utf-8')

            row['Job Id'] = anon_jobid

            writer.writerow(row)
