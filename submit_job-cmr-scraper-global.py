#!/usr/bin/env python

'''
Submits a standard job via a REST call
'''

from __future__ import print_function
import os
import json
import argparse
import requests
from datetime import date
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from hysds.celery import app


def main(job_name, short_name, job_params, job_version, queue, priority, tag_template):
    '''
    submits a job to mozart to start pager job
    '''
    # requests retry parameters
    retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], method_whitelist=["HEAD", "GET", "OPTIONS"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    Requests = requests.Session()
    Requests.mount("https://", adapter)
    Requests.mount("http://", adapter)

    # get volcano locations
    locations = query_volcano_locations()

    # loop through locations
    for volcano in locations.keys():
        location = locations[volcano]
        job_params["short_name"] = short_name
        job_params["location"] = location
        tags = "{}-{}-{}".format(tag_template, volcano, short_name)

        # submit mozart job
        job_submit_url = "{}{}".format(app.conf['MOZART_REST_URL'], '/job/submit')
        params = {
            'queue': queue,
            'priority': int(priority),
            'tags': '[{0}]'.format(parse_job_tags(tags)),
            'type': '%s:%s' % (job_name, job_version),
            'params': json.dumps(job_params),
            'enable_dedup': True
        }
        print('submitting jobs with params: %s' % json.dumps(params))
        r = Requests.post(job_submit_url, params=params, verify=False)
        if r.status_code != 200:
            print('submission job failed')
            r.raise_for_status()
        result = r.json()
        if 'result' in list(result.keys()) and 'success' in list(result.keys()):
            if result['success'] == True:
                job_id = result['result']
                print('submitted %s job version: %s job_id: %s' %
                    (job_name, job_version, job_id))
            else:
                raise Exception('job %s not submitted successfully: %s' %
                                (job_name, result))
        else:
            raise Exception('job %s not submitted successfully: %s' %
                            (job_name, result))


def parse_job_tags(tag_string):
    if tag_string == None or tag_string == '' or (type(tag_string) is list and tag_string == []):
        return ''
    tag_list = tag_string.split(',')
    tag_list = ['"{0}"'.format(tag) for tag in tag_list]
    return ','.join(tag_list)

def query_volcano_locations():
    '''
    Query GRQ ES for AOI-volcano locations
    '''
    # construct GRQ ES query
    endpoint = "grq_v1.0_aoi-volcano/_search"
    grq_es_url = "{}/{}".format(app.conf['GRQ_ES_URL'], endpoint)
    params = {"query": {"bool": {"must": [],"must_not": [],"should": [{"match_all": {}}]}},"from": 0,"size": 1500,"sort": [],"aggs": {}}
    print("submitting grq query {} with params: {}".format(grq_es_url,json.dumps(params)))

    # submit GRQ ES query
    r = requests.post(grq_es_url, params=params, verify=False)
    if r.status_code != 200:
        print('submission job failed')
        r.raise_for_status()
    result = r.json()
    hits = result["hits"]["hits"]

    # process results
    if len(hits) < 1:
        raise Exception('No hits from AVA GRQ ES')
    else:
        volcano_locations = {}
        for hit in hits:
            volcano_name = hit["_source"]["metadata"]["clean_name"]
            location = hit["_source"]["location"]
            volcano_locations[volcano_name] = location
    return volcano_locations




if __name__ == "__main__":
    today = date.today()
    date = today.strftime("%Y%m%d")
    default_tags = str(date) + 'automated-cmr-metadata-scrape';
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-j', '--job_name', help='Job name',
                        dest='job_name', required=False, default="job-scrape_aoi")
    parser.add_argument('-sn', '--short_name', help='Granule type (i.e AST_09T or AST_L1B)',
                        dest='short_name', required=True)
    parser.add_argument(
        '-p', '--params', help='Input params dict', dest='params', required=False, default={"cmr_enviorment": "PROD", "short_name": "", "location": ""})
    parser.add_argument('-v', '--version', help='release version, eg "master" or "release-20180615"',
                        dest='version', required=False, default='dev')
    parser.add_argument('-q', '--queue', help='Job queue', dest='queue',
                        required=False, default='factotum-job_worker-small')
    parser.add_argument('-pr', '--priority', help='Job priority',
                        dest='priority', required=False, default='2')
    parser.add_argument('-g', '--tags', help='Job tags. Use a comma separated list for more than one',
                        dest='tags', required=False, default=default_tags)
    args = parser.parse_args()
    main(args.job_name, args.short_name, args.params, args.version,
         args.queue, args.priority, args.tags)
