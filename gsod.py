#!/usr/bin/env python
import csv
import gzip
import datetime
import httplib2
from io import StringIO



class StationLoader(object):
    """
    Class which returns the list of stations by parsing the
    http://www.ncdc.noaa.gov/pub/data/gsod/ish-history.csv
    """
    STATIONS_URL = 'https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'

    def __init__(self, http_cache=None):
        self.cache = http_cache or '.cache'

    def get_stations(self, url=None):
        """
        Return the list of weather stations. One by one
        """
        h = httplib2.Http(self.cache)
        # ETag implementation on the server-side is broken
        h.ignore_etag = True
        resp, content = h.request(url or self.STATIONS_URL)
        if resp.status != 200:
            raise RuntimeError('Stations at {0} not found'.format(url or self.STATIONS_URL))
        reader = csv.reader(StringIO(content.decode('utf-8')))
        header = next(reader)
        field_names = self.get_field_names(header)
        for fields in reader:
            fields = (field.strip() or None for field in fields)
            obj = dict(zip(field_names, fields))
            obj = self.postprocess(obj)
            yield obj

    def get_field_names(self, header):
        """
        Convert upper case field names to more suitable dict keys
        """
        field_names = []
        for name in header:
            name = name.lower().replace(' ', '_').replace('(.1m)', '')
            field_names.append(name)
        return field_names

    def postprocess(self, obj):
        """
        Postprocess dict with station info
        """
        # latitude and longitude
        obj['lat'] = convert_location(obj['lat'])
        obj['lon'] = convert_location(obj['lon'])
        # elev
        obj['elev(m)'] = convert_location(obj['elev(m)'])
        # begin and end datetimes
        obj['begin'] = str_to_datetime(obj['begin'])
        obj['end'] = str_to_datetime(obj['end'])
        # return the object
        return obj


class WeatherLoader(object):

    WEATHER_URL_TMPL = 'http://www1.ncdc.noaa.gov/pub/data/gsod/{year}/{usaf}-{wban}-{year}.op.gz'

    def __init__(self, http_cache=None):
        self.cache = http_cache or '.cache'

    def get_weather(self, usaf, wban, year):
        """
        Get weather lines for every day in a given year for the station defined by its usaf and wban
        """
        url = self.WEATHER_URL_TMPL.format(year=year, usaf=usaf, wban=wban)
        h = httplib2.Http(self.cache)
        h.ignore_etag = True
        resp, content = h.request(url)
        if resp.status != 200:
            return
        gz = gzip.GzipFile(fileobj=StringIO(content))
        lines = gz.read().splitlines()[1:]
        for line in lines:
            obj = self.parse_weather_record(line)
            yield obj

    def parse_weather_record(self, line):
        """
        Parse one line of the weather record

        The parsing is done according to
        ftp://ftp.ncdc.noaa.gov/pub/data/gsod/readme.txt
        """
        field_names = ('date',
                       'temp', '_',
                       'dew_point', '_',
                       'sea_level_pressure', '_',
                       'station_pressure', '_',
                       'visibility', '_',
                       'wind_speed', '_',
                       'max_wind_speed',
                       'max_wind_gust',
                       'max_temp',  # has *
                       'min_temp',  # has *
                       'precipitation', # has flags A-I
                       'snow_depth',
                       'indicators')
        # list of fields which values have to be converted to floats
        float_field_names = (
            'temp', 'dew_point', 'sea_level_pressure', 'station_pressure',
            'visibility', 'wind_speed', 'max_wind_speed', 'max_wind_gust',
            'max_temp', 'min_temp', 'precipitation', 'snow_depth',
        )

        # base conversion
        fields = line.strip().split()[2:]
        fields = [field.strip() or None for field in fields]
        obj = dict(zip(field_names, fields))
        obj['date'] = str_to_datetime(obj['date'])
        # float conversion
        for field_name in float_field_names:
            value = obj[field_name].rstrip('*ABCDEFGHI')
            if value in ('9999.9', '99.99', '999.9'):
                value = None
            else:
                value = float(value)
            obj[field_name] = value
        # measurement units conversion
        temp_field_names = ('temp', 'max_temp', 'min_temp')
        for field_name in temp_field_names:
            celsius_field_name = field_name + '_c'
            value = obj[field_name]
            if value is not None:
                obj[celsius_field_name] = (value - 32) * 5 / 9
            else:
                obj[celsius_field_name] = None
        # parsing indicator values
        indicator_names = ('fog', 'rain', 'snow', 'hail', 'thunder', 'tornado')
        indicator_values = [flag == '1' for flag in obj['indicators']]
        indicator_obj = dict(zip(indicator_names, indicator_values))
        obj['weather_ok'] = not any(indicator_values)
        obj.update(indicator_obj)
        # finalization
        del obj['_']
        return obj


def str_to_datetime(val):
    return val and datetime.datetime.strptime(val, '%Y%m%d')


def convert_location(val):
    """Remove plus sign from lat/lon value if present"""
    if val:
        if val[0] == '+':
            return val.lstrip('+')
        return val
