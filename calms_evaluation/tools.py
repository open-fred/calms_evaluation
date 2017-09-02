import pandas as pd
import pickle
import copy
from oemof.db import coastdat
from feedinlib.weather import FeedinWeather
from shapely.geometry import Point
import dateutil.parser

import feedin


def fetch_geometries(conn, **kwargs):
    """
    Reads the geometry and the id of all given tables and writes it to
    the 'geom'-key of each branch of the data tree.
    """
    sql_str = '''
        SELECT {id_col}, ST_AsText(
            ST_SIMPLIFY({geo_col},{simp_tolerance})) geom
        FROM {schema}.{table}
        WHERE "{where_col}" {where_cond}
        ORDER BY {id_col} DESC;'''

    db_string = sql_str.format(**kwargs)
    results = conn.execute(db_string)
    cols = results.keys()
    return pd.DataFrame(results.fetchall(), columns=cols)


def fetch_shape_germany(conn):
    """
    Gets shape for Germany.
    """
    sql_str = '''
            SELECT ST_AsText(ST_Union(geom)) AS geom
            FROM deutschland.deu3_21'''
    return conn.execute(sql_str).fetchall()[0]


def create_multi_weather_from_merra_nc(conn, filename):
    """
    Reads dumped dataframe (created with OPSD weather data script) and makes
    multi_weather object (list of FeedinWeather objects from feedinlib) out of
    it.

    Parameters
    ----------
    filename : String
        Filename with path to dumped dataframe.
    """

    df_merra = pd.read_pickle(filename)
    # drop columns that are not needed
    df_merra = df_merra.drop(['v1', 'v2', 'h1', 'h2', 'cumulated hours'], 1)

    # get all distinct pairs of latitude and longitude in order to create
    # one FeedinWeather object for each data point
    df_lat_lon = df_merra.groupby(['lat', 'lon']).size().reset_index().drop(0,
        1)

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
        feedin_object = FeedinWeather(data=copy.deepcopy(data_df),
            timezone=data_df.index.tz,
            longitude=longitude, latitude=latitude,
            geometry=geom, data_height=data_height,
            name=name)
        multi_weather.append(copy.deepcopy(feedin_object))
    return multi_weather


def get_data(conn=None, power_plant=None, multi_weather=None, year=None,
             geom=None, pickle_load=True, filename='pickle_dump.p',
             data_type='multi_weather_coastdat'):
    if not pickle_load:
        if data_type == 'multi_weather_coastdat':
            data = coastdat.get_weather(conn, geom, year)
        elif data_type == 'multi_weather_merra':
            data = create_multi_weather_from_merra_nc(conn, filename)
            filename = 'multiweather_merra_' + str(year) + '.p'  # filename for dump
        elif data_type == 'wind_feedin':
            data = {}
            for i in range(len(multi_weather)):
                data[multi_weather[i].name] = power_plant.feedin(
                    weather=multi_weather[i], installed_capacity=1)
                data[multi_weather[i].name].name = 'feedin'
        elif data_type == 'pv_feedin':
            data = feedin.pv(multi_weather, **power_plant)
        pickle.dump(data, open(filename, 'wb'))
    if pickle_load:
        data = pickle.load(open(filename, 'rb'))
    return data


