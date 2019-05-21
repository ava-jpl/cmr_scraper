#!/usr/bin/env python

'''
Queries the CMR for relevant metadata, ingesting the resulting product.
'''

from __future__ import print_function
import os
import json
import math
import shutil
import urllib3
import dateutil.parser
import requests
from hysds.celery import app
from hysds.dataset_ingest import ingest

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VERSION = "v1.0"
PROD = "MET-{}-{}-{}"
PROD_TYPE = "grq_{}_metadata-{}"
CMR_URL = 'https://cmr.earthdata.nasa.gov'


def main():
    '''
    Scrapes the CMR over a given starttime, endtime, & location geojson for
    a given product shortname. Ingests the result.
    '''
    # load parameters
    ctx = load_context()
    starttime = ctx.get("starttime", False)
    endtime = ctx.get("endtime", False)
    location = ctx.get("location", False)
    shortname = ctx.get("short_name", False)
    if not shortname:
        raise Exception("short_name must be specified.")
    # build query
    temporal_str = gen_temporal_str(starttime, endtime)
    polygon_str = gen_spatial_str(location)
    url = "{}/search/granules.json?page_size=2000{}{}&short_name={}&scroll=true".format(CMR_URL, temporal_str, polygon_str, shortname)
    # run query
    results_list = run_query(url, verbose=2)
    for result in results_list:
        # generate product
        ds, met = gen_product(result, shortname)
        # ingest product
        #ingest_product(ds, met)
        shortname = met.get('short_name', '*')
        uid = ds.get('label')
        if exists(uid, shortname):
            continue
        save_product_met(uid, ds, met)

def gen_temporal_str(starttime, endtime):
    '''generates the temporal string for the cmr query'''
    start_str = ''
    end_str = ''
    if starttime:
        start_str = dateutil.parser.parse(starttime).strftime('%Y-%m-%dT%H:%M:%SZ')
    if endtime:
        end_str = dateutil.parser.parse(endtime).strftime('%Y-%m-%dT%H:%M:%SZ')
    # build query
    temporal_span = ''
    if starttime or endtime:
        temporal_span = '&temporal={0},{1}'.format(start_str, end_str)
    return temporal_span

def gen_spatial_str(location):
    '''generates the spatial string for the cmr query'''
    if not location:
        return ''
    coords = location['coordinates'][0]
    if get_area(coords) > 0: #reverse orde1r if not clockwise
        coords = coords[::-1]
    coord_str = ','.join([','.join([format_digit(x) for x in c]) for c in coords])
    return '&polygon={}'.format(coord_str)
    
def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return area / 2

def format_digit(digit):
    return "{0:.8g}".format(digit)

def gen_product(result, shortname):
    '''generates a dataset.json and met.json dict for the product'''
    starttime = result["time_start"]
    dt_str = dateutil.parser.parse(starttime).strftime('%Y%m%d')
    endtime = result["time_end"]
    location = parse_location(result)
    prod_id = gen_prod_id(shortname, starttime, endtime)
    ds = {"label": prod_id, "starttime": starttime, "endtime": endtime, "location": location, "version": VERSION}
    met = result
    met['shortname'] = shortname
    return ds, met

def gen_prod_id(shortname, starttime, endtime):
    '''generates the product id from the input metadata & params'''
    start = dateutil.parser.parse(starttime).strftime('%Y%m%dT%H%M%S')
    end = dateutil.parser.parse(endtime).strftime('%Y%m%dT%H%M%S')
    time_str = '{}_{}'.format(start, end)
    return PROD.format(shortname, time_str, VERSION)

def ingest_product(ds, met):
    '''publish a product directly'''
    uid = ds['label']
    shortname = met.get('short_name', '*')
    save_product_met(uid, ds, met)
    ds_dir = os.path.join(os.getcwd(), uid)
    if exists(uid, shortname):
        print('Product already exists with uid: {}. Passing on publish...'.format(uid))
        return
    print('Product with uid: {} does not exist. Publishing...'.format(uid))
    try:
        ingest(uid, './datasets.json', app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, ds_dir, None) 
        if os.path.exists(uid):
            shutil.rmtree(uid)
    except:
        raise Exception('failed on submission of {0}'.format(uid))

def parse_location(result):
    '''parse out the geojson from the CMR return'''
    poly = result["polygons"][0][0]
    coord_list = poly.split(' ')
    coords = [[[float(coord_list[i+1]), float(coord_list[i])] for i in range(0, len(coord_list), 2)]]
    location = {"type": "Polygon", "location": coords}
    return location

def get_session(verbose=False):
    '''returns a CMR requests session'''
    #user = os.getenv('CMR_USERNAME')
    #passwd = os.getenv('CMR_PASSWORD')
    #if user is None or passwd is None:
    #    if verbose > 1:
    #        print ("Environment username & password not found")
    return requests.Session()
    #token_url = os.path.join(cmr_url, 'legacy-services/rest/tokens')
    #if verbose: print('token_url: %s' % token_url)
    #headers = {'content-type': 'application/json'}
    #info = {'token': {'username': user, 'password': passwd, 'client_id': user, 'user_ip_address': '127.0.0.1'}}
    #data = json.dumps(info, indent=2)
    #if verbose: print(data)
    #session = requests.Session()
    #r = session.post(token_url, data=data, headers=headers)
    #if verbose: print(r.text)
    #r.raise_for_status()
    #robj = xml.etree.ElementTree.fromstring(r.text)
    #token = robj.find('id').text
    #if verbose: print("using session token:".format(token))
    #return session

def run_query(query_url, verbose=False):
    """runs a scrolling query over the given url and returns the result as a dictionary"""
    if verbose:
        print('querying url: {0}'.format(query_url))
    granule_list = []
    session = get_session(verbose=verbose)
    #initial query
    response = session.get(query_url)
    response.raise_for_status()
    granule_list.extend(json.loads(response.text)["feed"]["entry"])
    #get headers for scrolling
    tot_granules = response.headers["CMR-Hits"]
    scroll_id = response.headers["CMR-Scroll-Id"]
    headers = {'CMR-Scroll-Id' : scroll_id}
    if len(granule_list) is 0:
        if verbose > 0:
            print('no granules returned')
        return []
    pages = int(math.ceil(float(tot_granules) / len(granule_list)))
    if verbose > 1:
        print("total granules matching query: {0}".format(tot_granules))
        print("Over {0} pages".format(pages))
        print("Using scroll-id: {0}".format(scroll_id))
    if verbose > 2:
        print("response text: {0}".format(response.text))
        print("response headers: {0}".format(response.headers))
    for i in range(1, pages):
        if verbose > 1:
            print("querying page {0}".format(i+1))
        response = session.get(query_url, headers=headers)
        response.raise_for_status()
        granule_returns = json.loads(response.text)["feed"]["entry"]
        if verbose > 1:
            print("query returned {0} granules".format(len(granule_returns)))
        if verbose > 2:
            print("response text: {0}".format(response.text))
            print("response headers: {0}".format(response.headers))
            print('with {0} granules'.format(len(granule_returns)))
        granule_list.extend(granule_returns)
    #text = json.dumps(granule_list, sort_keys=True, indent=4, separators=(',', ': '))
    if verbose:
        print("query returned {0} total granules".format(len(granule_list)))
    if len(granule_list) != int(tot_granules):
        raise Exception("Total granules returned from query do not match expected granule count")
    return granule_list

def save_product_met(prod_id, ds_obj, met_obj):
    '''generates the appropriate product json files in the product directory'''
    if not os.path.exists(prod_id):
        os.mkdir(prod_id)
    outpath = os.path.join(prod_id, '{}.dataset.json'.format(prod_id))
    with open(outpath, 'w') as outf:
        json.dump(ds_obj, outf)
    outpath = os.path.join(prod_id, '{}.met.json'.format(prod_id))
    with open(outpath, 'w') as outf:
        json.dump(met_obj, outf)


def exists(uid, shortname):
    '''queries grq to see if the input id exists. Returns True if it does, False if not'''
    grq_ip = app.conf['GRQ_ES_URL']#.replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/{1}/_search'.format(grq_ip, PROD_TYPE.format(VERSION, shortname))
    es_query = {"query":{"bool":{"must":[{"term":{"id.raw":uid}}]}},"from":0,"size":1}
    return query_es(grq_url, es_query)

def query_es(grq_url, es_query):
    '''simple single elasticsearch query, used for existence. returns count of result.'''
    print('querying: {} with {}'.format(grq_url, es_query))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    try:
        response.raise_for_status()
    except:
        # if there is an error (or 404,just publish
        return 0
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    return int(total_count)

def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')

if __name__ == '__main__':
    main()
