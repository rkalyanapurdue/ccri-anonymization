#!/bin/env python3

import redis
import csv

r = redis.StrictRedis(host='localhost', port=6379, db=0)

with open('/data/job_accounting_nov_hosts.csv','r') as srcfile:
    reader = csv.DictReader(srcfile)
    fieldnames = reader.fieldnames

    with open('/data/job_accounting_nov_anon.csv','w') as resfile:
        writer = csv.DictWriter(resfile,fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:

            hosts = row['Hosts']
            anon_hosts = []
            
            for host in hosts.split(','):
                host_key = 'HOST_%s' % host

                if not r.exists(host_key):
                    r.incr('node')

                    anon_val = '%s%s' % ('NODE',r.get('node').decode('utf-8'))

                    r.set(host_key,anon_val)

                anon_hosts.append(r.get(host_key).decode('utf-8'))

            row['Hosts'] = ','.join(anon_hosts)

            #host = row['Host']
            #host_key = 'HOST_%s' % host

            #if not r.exists(host_key):
            #    r.incr('node')
            #    anon_val = '%s%s' % ('NODE',r.get('node').decode('utf-8'))
            #    r.set(host_key,anon_val)

            #anon_host = r.get(host_key).decode('utf-8')

            #row['Host'] = anon_host

            job_id = row['Job Id']
            jobid_key = 'JOB_%s' % job_id

            if not r.exists(jobid_key):
                r.incr('job')
                anon_val = '%s%s' % ('JOB',r.get('job').decode('utf-8'))
                r.set(jobid_key,anon_val)

            anon_jobid = r.get(jobid_key).decode('utf-8')

            row['Job Id'] = anon_jobid

            group_id = row['Account']
            gid_key = 'GROUP_%s' % group_id

            if not r.exists(gid_key):
                r.incr('group')
                anon_val = '%s%s' % ('GROUP',r.get('group').decode('utf-8'))
                r.set(gid_key,anon_val)

            anon_gid = r.get(gid_key).decode('utf-8')

            row['Account'] = anon_gid

            user_id = row['User']
            uid_key = 'USER_%s' % user_id

            if not r.exists(uid_key):
                r.incr('user')
                anon_val = '%s%s' % ('USER',r.get('user').decode('utf-8'))
                r.set(uid_key,anon_val)

            anon_uid = r.get(uid_key).decode('utf-8')

            row['User'] = anon_uid

            writer.writerow(row)
