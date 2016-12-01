import numpy as np
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta

class Agg:
    def __init__(self):
        gojek = MongoClient().gojek
        self.data = pd.DataFrame(
                [c for c in gojek.clean.find({}, {
                    '_id': 0,
                    'cancelTime': 1,
                    'dispatchTime': 1,
                    'latOrigin': 1,
                    'longOrigin': 1
                    })])

        self.data['idTime'] = self.data.cancelTime.fillna(self.data.dispatchTime)
        self.data['isCancel'] = self.data.cancelTime.notnull()
        self.data.drop(['cancelTime', 'dispatchTime'], axis=1, inplace=True)
        self.data.dropna(how='any', inplace=True)

    def aggregate1(self, req):
        lat_from = req['lat_from']
        long_from = req['long_from']
        lat_to = req['lat_to']
        long_to = req['long_to']
        time_from = req['time_from']
        time_to = req['time_to']
        cell = req['n_items']**0.5

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
                bins=[np.linspace(lat_from, lat_to, cell),
                    np.linspace(long_from, long_to, cell)])

        arg = np.argwhere(hist)
        hist[:] = np.sqrt(hist/hist.max())
        return np.dstack([x[arg[:,0]], y[arg[:,1]], hist[hist > 0]]).tolist()[0]

    def aggregate2(self, req):
        lat_from = req['lat_from']
        long_from = req['long_from']
        lat_to = req['lat_to']
        long_to = req['long_to']
        time_from = req['time_from']
        time_to = req['time_to']
        cell = req['n_items']**0.5

        day = datetime(2015,11,23)
        start = day + timedelta(hours=time_from)
        end = day + timedelta(hours=time_to)
        df = self.data.loc[(self.data.idTime > start) &
                (self.data.idTime < end) &
                (self.data.latOrigin > lat_from) &
                (self.data.latOrigin < lat_to) &
                (self.data.longOrigin > long_from) &
                (self.data.longOrigin < long_to)]

        # compare each coordinate to a grid and calc which cell it's closest to
        x = np.linspace(lat_from, lat_to, cell)
        y = np.linspace(long_from, long_to, cell)
        dx = x[np.abs(np.subtract.outer(df.latOrigin, x)).argmin(axis=-1)]
        dy = y[np.abs(np.subtract.outer(df.longOrigin, y)).argmin(axis=-1)]

        count = df.groupby([dx, dy]).latOrigin.count()
        count[:] = (count.astype(float)/count.max())**0.5

        return [[k[0], k[1], v] for k, v in count.iteritems()]

