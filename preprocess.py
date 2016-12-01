#!/usr/bin/python2

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
import re

client = MongoClient()
bookings = client['gojek']['bookings']

default_keys=['dispatchTime', 'arrivalTime', 'closingTime', 'driverCalledTime', 'cancelTime', 'timeField', 'driverLatitude', 'driverLongitude', 'driverPickupLocation', 'driverCloseLocation']

def pp_tables():
    # get table names
    c.execute("select name from sqlite_master where type='table';")
    names = c.fetchall()
    # function to parse rows
    p = re.compile(r'(?<![0-9]|:|,|\{)"(?![0-9]|:|,"|\})')
    #parse = lambda r: eval(p.sub('', r[r.find('{'):r.rfind('}')+1].replace(':""',':None').replace(':null',':None').replace(':true',':True').replace(':false',':False')))
    parse = json.loads

    tables = {}
    errors = {}
    for t in names:
        c.execute("select * from %s" % t)
        print >> sys.stderr, "table: %s" % t[0]
        row = []
        err = []
        for n, r in c.fetchall():
            try:
                row.append(parse(p.sub('',r.replace('\t',' '))))
            except:
                err.append(n)
        print('len: ' + str(len(row)) + ' err: ' + str(len(err)))
        errors[t[0]] = err
        tables[t[0]] = pd.DataFrame(row)
        tables[t[0]].dropna(axis=1, how='all', inplace=True)
    del row, err
    return tables, errors
    
def get_bookings(keys=None, chunksize=100000):
    import MySQLdb as sq
    from django.utils.dateparse import parse_datetime
    import json

    db = sq.connect(host='localhost', user='root', passwd='root', db='gojek', unix_socket='/run/mysqld/mysqld.sock')
    c = db.cursor()

    p = re.compile(r'(?<![0-9]|:|,|\{)"(?![0-9]|:|,"|\})')
    datetimes = ['timeField', 'cancelTime', 'feedbackTime']
    offset = 0
    while True:
        book = []
        address = []
        success = 0
        fail = 0
        sql = "select * from bookings limit %d offset %d;" % (chunksize, offset)
        c.execute(sql)
        for n, r in c.fetchall():
            try:
                assert r.find('Internal Server Error') < 0
                start = r.find('routePolyline')
                end = r.find('driverCloseLocation')
                r = r[:start] + r[end:]
                row = json.loads(p.sub('', r), strict=False)

                if keys:
                    [row.pop(k) for k in row.keys() if k not in keys]

                for k in row.keys():
                    if k == 'timeField' and row[k]:
                        row[k] = parse_datetime(row[k])
                    elif k.endswith('Time') and row[k]:
                        try: row[k] = datetime.fromtimestamp(row[k]/1000)
                        except: row[k] = parse_datetime(row[k])
                    elif k == 'addresses':
                        add = row[k][0]
                        if add['closeTime']:
                            add['closeTime'] = parse_datetime(add['closeTime'])
                        if add['latLongDestination']:
                            lat, lon = add['latLongDestination'].split(',')
                            add['latDestination'] = float(lat)
                            add['longDestination'] = float(lon)
                            add.pop('latLongDestination')
                        address.append(add)

                book.append(row)
                success += 1
            except:
                fail += 1
        try:
            bookings.insert_many(book)
            if len(address) > 0:
                addresses.insert_many(address)
            print(str(offset) + ': success')
            print('parsed / failed')
            print(str(success) + ' / ' + str(fail))
        except:
            print(str(offset) + ': failed')

        if success+fail < chunksize: break
        offset += chunksize

def pp_addresses():
    add = pd.DataFrame([a for a in addresses.find({}, {
        '_id':0, 'closeTime':1, 'latDestination':1, 'longDestination':1})])

    jkt = [-6.21462, 106.84513]


def pp_bookings():
    loc = []
    cancelled = 0
    for b in book:
        if b:
            try:
                loc.append(b.values()[0].split(','))
            except:
                cancelled += 1
    print('booked / cancelled')
    print(str(len(loc)) + ' / ' + str(len(cancelled)))

    loc = np.array(loc, dtype=float)
    jkt = [-6.21462, 106.84513]
    loc = loc[np.all(loc - jkt < 1., axis=-1)]
    return pd.DataFrame(loc, columns=['lat', 'long'])

def agg_bookings(req, res):
    req = json.loads(json_in)
    lat0 = req['lat0']
    long0 = req['long0']
    lat1 = req['lat1']
    long1 = req['long1']
    time_from = req['time_from']
    time_to = req['time_to']
    n_items = req['n_items']
    
    # compare each location to a grid and calc which cell it's closest to
    x = np.abs(np.subtract.outer(df_in.lat.values,
        np.linspace(df_in.lat.min(), df_in.lat.max(), cell)))
    y = np.abs(np.subtract.outer(df_in.long.values,
        np.linspace(df_in.long.min(), df_in.long.max(), cell)))

    df_in['x'] = x.argmin(axis=-1)
    df_in['y'] = y.argmin(axis=-1)

    count = df_in.groupby(['x','y']).lat.count()

    if json_out:
        count.to_json(json_out)
    else:
        return count

def get_loc():
    import requests
    clean = MongoClient().gojek.clean
    data = pd.DataFrame([c for c in clean.find({}, {'_id':0, 'latOrigin':1, 'longOrigin':1, 'latDestination':1, 'longDestination':1})])
    orig = data[['latOrigin', 'longOrigin']].astype(str).values.tolist()
    dest = data[['latDestination', 'longDestination']].astype(str).values.tolist()
    api = 'https://maps.googleapis.com/maps/api/geocode/json'
    key = 'AIzaSyADRaHt8UYaaQqTZM4F5GH4HFgCzIXjJVw'
    result_type = 'street_address'
    payload = {'key':key, 'result_type':result_type}

    orig_full = []
    dest_full = []
    fail = []
    for n in range(len(orig)):
        try:
            payload['latlng'] = ','.join(orig[n])
            r = requests.get(api, payload)
            res = r.json()
            assert res['status'] == 'OK'
            orig_full.append(res['results'])

            payload['latlng'] = ','.join(dest[n])
            r = requests.get(api, payload)
            res = r.json()
            assert res['status'] == 'OK'
            dest_full.append(res['results'])

            print(n, res['results'][0]['formatted_address'])
        except:
            fail.append(n)
        
    return orig_full, dest_full, fail

def agg_delta(self, req):
    lat_from = req['lat_from']
    long_from = req['long_from']
    lat_to = req['lat_to']
    long_to = req['long_to']
    time_from = req['time_from']
    time_to = req['time_to']
    delta = req['delta']

    lat_n = (lat_to - lat_from)/delta
    long_n = (long_to - long_from)/delta

    day = datetime(2015,11,23)
    start = day + timedelta(hours=time_from)
    end = day + timedelta(hours=time_to)
    df = self.data.loc[(self.data.idTime > start) &
            (self.data.idTime < end) &
            (self.data.latOrigin > lat_from) &
            (self.data.latOrigin < lat_to) &
            (self.data.longOrigin > long_from) &
            (self.data.longOrigin < long_to)]

    hist, x, y = np.histogram2d(x=df.latOrigin, y=df.longOrigin,
            bins=[np.linspace(lat_from, lat_to, lat_n),
                np.linspace(long_from, long_to, long_n)])

    arg = np.argwhere(hist)
    hist[:] = np.sqrt(hist/hist.max())
    return np.dstack([x[arg[:,0]], y[arg[:,1]], hist[hist > 0]]).tolist()[0]

