import copy
import logging
import pandas as pd
import numpy as np
from pytz import timezone
from datetime import datetime
from oemof.db import tools as oemof_tools
from shapely.wkt import loads as wkt_loads
from shapely.geometry import Point
import dateutil.parser

# ToDo: Document module!


class FeedinWeather:
    def __init__(self, **kwargs):
        r"""
        Class, containing all meta information regarding the weather data set.

        Parameters
        ----------
        data : pandas.DataFrame, optional
            Containing the time series of the different parameters as columns
        timezone : string, optional
            Containing the name of the time zone using the naming of the
            IANA (Internet Assigned Numbers Authority) time zone database [1]_
        longitude : float, optional
            Longitude of the location of the weather data
        latitude : float, optional
            Latitude of the location of the weather data
        geometry : shapely.geometry object
            polygon or point representing the zone of the weather data
        data_height : dictionary, optional
            Containing the heights of the weather measurements or weather
            model in meters with the keys of the data parameter
        name : string
            Name of the weather data object

        Notes
        -----
        Depending on the used feedin model some of the optional parameters
        might be mandatory.

        References
        ----------
        .. [1] `IANA time zone database <http://www.iana.org/time-zones>`_.

        """
        self.data = kwargs.get('data', None)
        try:
            self.timezone = self.data.index.tz
        except:
            self.timezone = kwargs.get('timezone', None)
        self.longitude = kwargs.get('longitude', None)
        self.latitude = kwargs.get('latitude', None)
        self.geometry = kwargs.get('geometry', None)
        self.data_height = kwargs.get('data_height', None)
        self.name = kwargs.get('name', None)


def create_feedinweather_objects_merra(conn, filename):
    """
    Reads dumped pd.DataFrame containing MERRA2 weather data for one year
    (created with OPSD weather data script [1]) and generates a list of
    FeedinWeather objects.

    Parameters
    ----------
    conn : sqlalchemy connection object
        Use function `connection` from oemof.db to establish database
        connection.
    filename : String
        Filename with path to dumped dataframe.

    References
    ----------
    .. [1] OPSD weather data github
    """

    # ToDo: add link to OPSD
    # ToDo: check if units are the same as for coastdat data

    df_merra = pd.read_pickle(filename)
    # drop columns that are not needed
    df_merra = df_merra.drop(
        ['v1', 'v2', 'h1', 'h2', 'cumulated hours', 'rho'], 1)
    # ToDo: Vorher noch GHI und DHI berechnen
    df_merra.rename(columns={'SWTDN': 'dhi',
                             'SWGDN': 'dirhi'},
                    inplace=True)
    # get all distinct pairs of latitude and longitude in order to create
    # one FeedinWeather object for each data point
    df_lat_lon = df_merra.groupby(
        ['lat', 'lon']).size().reset_index().drop(0, 1)

    # make timeindex once to use for each multi_weather entry
    # get timestamps for one data point
    timestamp_series = df_merra[(df_merra.lat == df_lat_lon.loc[0, 'lat']) &
                                (df_merra.lon == df_lat_lon.loc[0, 'lon'])][
        'timestamp']
    # parse timestamp string to timezone aware datetime object
    timestamp_series = timestamp_series.apply(
        lambda x: dateutil.parser.parse(x))

    # get geometry gids for each data point from db
    sql_str = '''
        SELECT gid, ST_X(geom) AS long, ST_Y(geom) AS lat
        FROM public.merra_grid;'''
    results = conn.execute(sql_str)
    cols = results.keys()
    merra_gid_df = pd.DataFrame(results.fetchall(), columns=cols)

    # create FeedinWeather object for each data point (lat-lon pair)
    multi_weather = []
    for i in range(len(df_lat_lon)):
        data_df = df_merra[(df_merra.lat == df_lat_lon.loc[i, 'lat']) &
                           (df_merra.lon == df_lat_lon.loc[i, 'lon'])]
        data_df = data_df.drop(['lat', 'lon', 'timestamp'], 1)
        data_df = data_df.set_index(timestamp_series)
        data_df = data_df.rename(columns={'v_50m': 'v_wind',
                                          'T': 'temp_air',
                                          'p': 'pressure'})
        longitude = df_lat_lon.loc[i, 'lon']
        latitude = df_lat_lon.loc[i, 'lat']
        geom = Point(longitude, latitude)
        data_height = {'v_wind': 50,
                       'temp_air': 2,
                       'dhi': 0,
                       'dirhi': 0,
                       'pressure': 0,
                       'Z0': 0}
        name = int(
            merra_gid_df[(merra_gid_df['long'] == longitude) & (
                          merra_gid_df['lat'] == latitude)].iloc[0]['gid'])
        feedin_object = FeedinWeather(
            data=copy.deepcopy(data_df),
            timezone=data_df.index.tz,
            longitude=longitude, latitude=latitude,
            geometry=geom, data_height=data_height,
            name=name)
        multi_weather.append(copy.deepcopy(feedin_object))
    return multi_weather


def create_feedinweather_objects_coastdat(conn, geometry, year):
    r"""
    Get the weather data for the given geometry and create an oemof
    weather object.
    """
    rename_dc = {
        'ASWDIFD_S': 'dhi',
        'ASWDIR_S': 'dirhi',
        'PS': 'pressure',
        'T_2M': 'temp_air',
        'WSS_10M': 'v_wind',
        'Z0': 'z0'}

    if geometry.geom_type in ['Polygon', 'MultiPolygon']:
        # Create MultiWeather
        # If polygon covers only one data set switch to SingleWeather
        sql_part = """
            SELECT sp.gid, ST_AsText(point.geom), ST_AsText(sp.geom)
            FROM coastdat.cosmoclmgrid AS sp
            JOIN coastdat.spatial AS point ON (sp.gid=point.gid)
            WHERE st_intersects(ST_GeomFromText('{wkt}',4326), sp.geom)
            """.format(wkt=geometry.wkt)
        df = fetch_raw_data(sql_weather_string(year, sql_part), conn, geometry)
        obj = create_multi_weather(df, rename_dc)
    elif geometry.geom_type == 'Point':
        # Create SingleWeather
        sql_part = """
            SELECT sp.gid, ST_AsText(point.geom), ST_AsText(sp.geom)
            FROM coastdat.cosmoclmgrid AS sp
            JOIN coastdat.spatial AS point ON (sp.gid=point.gid)
            WHERE st_contains(sp.geom, ST_GeomFromText('{wkt}',4326))
            """.format(wkt=geometry.wkt)
        df = fetch_raw_data(sql_weather_string(year, sql_part), conn, geometry)
        obj = create_single_weather(df, rename_dc)
    else:
        logging.error('Unknown geometry type: {0}'.format(geometry.geom_type))
        obj = None
    return obj


def sql_weather_string(year, sql_part):
    """
    Creates an sql-string to read all datasets within a given geometry.
    """

    # Create string parts for where conditions
    return '''
    SELECT tsptyti.*, y.leap
    FROM coastdat.year as y
    INNER JOIN (
        SELECT tsptyd.*, sc.time_id
        FROM coastdat.scheduled as sc
        INNER JOIN (
            SELECT tspty.*, dt.name, dt.height
            FROM coastdat.datatype as dt
            INNER JOIN (
                SELECT tsp.*, typ.type_id
                FROM coastdat.typified as typ
                INNER JOIN (
                    SELECT spl.*, t.tsarray, t.id
                    FROM coastdat.timeseries as t
                    INNER JOIN (
                        SELECT sps.*, l.data_id
                        FROM (
                            {sql_part}
                            ) as sps
                        INNER JOIN coastdat.located as l
                        ON (sps.gid = l.spatial_id)) as spl
                    ON (spl.data_id = t.id)) as tsp
                ON (tsp.id = typ.data_id)) as tspty
            ON (tspty.type_id = dt.id)) as tsptyd
        ON (tsptyd.id = sc.data_id))as tsptyti
    ON (tsptyti.time_id = y.year)
    where y.year = '{year}'
    ;'''.format(year=year, sql_part=sql_part)


def fetch_raw_data(sql, connection, geometry):
    """
    Fetch the coastdat2 from the database, adapt it to the local time zone
    and create a time index.
    """
    tmp_dc = {}
    weather_df = pd.DataFrame(
        connection.execute(sql).fetchall(), columns=[
            'gid', 'geom_point', 'geom_polygon', 'data_id', 'time_series',
            'dat_id', 'type_id', 'type', 'height', 'year', 'leap_year']).drop(
        'dat_id', 1)

    # Get the timezone of the geometry
    tz = oemof_tools.tz_from_geom(connection, geometry)

    for ix in weather_df.index:
        # Convert the point of the weather location to a shapely object
        weather_df.loc[ix, 'geom_point'] = wkt_loads(
            weather_df['geom_point'][ix])

        # Roll the dataset forward according to the timezone, because the
        # dataset is based on utc (Berlin +1, Kiev +2, London +0)
        utc = timezone('utc')
        offset = int(utc.localize(datetime(2002, 1, 1)).astimezone(
            timezone(tz)).strftime("%z")[:-2])

        # Get the year and the length of the data array
        db_year = weather_df.loc[ix, 'year']
        db_len = len(weather_df['time_series'][ix])

        # Set absolute time index for the data sets to avoid errors.
        tmp_dc[ix] = pd.Series(
            np.roll(np.array(weather_df['time_series'][ix]), offset),
            index=pd.date_range(pd.datetime(db_year, 1, 1, 0),
                                periods=db_len, freq='H', tz=tz))
    weather_df['time_series'] = pd.Series(tmp_dc)
    return weather_df


def create_single_weather(df, rename_dc):
    """Create an oemof weather object for the given geometry"""
    my_weather = FeedinWeather()
    data_height = {}
    name = None
    # Create a pandas.DataFrame with the time series of the weather data set
    weather_df = pd.DataFrame(index=df.time_series.iloc[0].index)
    for row in df.iterrows():
        key = rename_dc[row[1].type]
        weather_df[key] = row[1].time_series
        data_height[key] = row[1].height if not np.isnan(row[1].height) else 0
        name = row[1].gid
    my_weather.data = weather_df
    my_weather.timezone = weather_df.index.tz
    my_weather.longitude = df.geom_point.iloc[0].x
    my_weather.latitude = df.geom_point.iloc[0].y
    my_weather.geometry = df.geom_point.iloc[0]
    my_weather.data_height = data_height
    my_weather.name = name
    return my_weather


def create_multi_weather(df, rename_dc):
    """Create a list of oemof weather objects if the given geometry is a polygon
    """
    weather_list = []
    # Create a pandas.DataFrame with the time series of the weather data set
    # for each data set and append them to a list.
    for gid in df.gid.unique():
        gid_df = df[df.gid == gid]
        obj = create_single_weather(gid_df, rename_dc)
        weather_list.append(obj)
    return weather_list
