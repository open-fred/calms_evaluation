import pandas as pd
import pickle
import copy
from oemof.db import coastdat
from feedinlib.weather import FeedinWeather
from shapely.geometry import Point
import dateutil.parser

import feedin


def fetch_geometries_from_db(conn, **kwargs):
    r"""
    Gets spatial geometry from database.

    Parameters
    ----------
    conn : sqlalchemy connection object
        Use function `connection` from oemof.db to establish database
        connection.
    kwargs :
        id_col : String
            Name of column containing distinct values to use as ID.
        geo_col : String
            Name of column containing the geometry.
        schema : String
            Name of schema.
        table : String
            Name of table.
        where_col : String
            Name of column containing the information used for the where
            statement.
        where_cond : String
            Where condition statement (e.g. '> 0').
        simp_tolerance : String

    Returns
    -------
    pd.DataFrame
        DataFrame with one column 'gid' holding the values of the `id_col` and
        one column 'geom' holding the geometries.

    """
    sql_str = '''
        SELECT {id_col} gid, ST_AsText(
            ST_SIMPLIFY({geo_col},{simp_tolerance})) geom
        FROM {schema}.{table}
        WHERE "{where_col}" {where_cond}
        ORDER BY {id_col} DESC;'''
    db_string = sql_str.format(**kwargs)
    results = conn.execute(db_string)
    return pd.DataFrame(results.fetchall(), columns=results.keys())


def fetch_shape_germany_from_db(conn):
    r"""
    Gets shape of Germany from database.

    Parameters
    ----------
    conn : sqlalchemy connection object
        Use function `connection` from oemof.db to establish database
        connection.

    Returns
    -------
    string
        Shape of Germany as well-known text .

    """
    sql_str = '''
        SELECT ST_AsText(ST_Union(geom)) AS geom
        FROM deutschland.deu3_21'''
    return conn.execute(sql_str).fetchall()[0]


def create_merra_multi_weather(conn, filename):
    """
    Reads dumped pd.DataFrame containing MERRA2 weather data for one year
    (created with OPSD weather data script [1]) and generates `multi_weather`
    (list of FeedinWeather objects).

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

    #ToDo: add link to OPSD
    #ToDo: check if units are the same as for coastdat data

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


def get_weather_data(pickle_load, filename='pickle_dump.p', weather_data=None,
                     conn=None, year=None, geom=None):
    """
    Helper function to load pickled weather data or retrieve data and dump it.

    Parameters
    ----------
    pickle_load : Boolean
        True if data has already been dumped before.
    filename : String
        Name (including path) of file to load data from or if
        MERRA data is retrieved using function 'create_merra_multi_weather'
        to get data from. Default: 'pickle_dump.p'.
    weather_data : String
        String specifying if coastdat or MERRA data is retrieved in case
        `pickle_load` is False. Default: None.
    conn : sqlalchemy connection object
        Use function `connection` from oemof.db to establish database
        connection. Default: None.
    year : int
        Specifies which year the weather data is retrieved for. Default: None.
    geom : shapely.Geometry
        Region coastdat weather data is obtained for. Default: None.

    Returns
    -------
    list
        `multi_weather` is a list of :class:`feedinlib.weather.FeedinWeather`
        objects.

    """

    # ToDo: check type of geom

    if pickle_load:
        data = pickle.load(open(filename, 'rb'))
    else:
        if weather_data == 'coastdat':
            data = coastdat.get_weather(conn, geom, year)
        elif weather_data == 'merra':
            data = create_merra_multi_weather(conn, filename)
            filename = 'multiweather_merra_' + str(year) + '.p'
        pickle.dump(data, open(filename, 'wb'))
    return data


def get_feedin_data(pickle_load, filename='pickle_dump.p', type=None,
                    multi_weather=None,  power_plant=None,
                    weather_data_height=None):
    """
    Helper function to load pickled feed-in data or retrieve data and dump it.

    Parameters
    ----------
    pickle_load : Boolean
        True if data has already been dumped before.
    filename : String
        Name (including path) of file to load data from or dump data to.
        Default: 'pickle_dump.p'.
    type : String
        String specifying if PV or wind feed-in data is calculated.
        Default: None.
    multi_weather : list
        `multi_weather` is a list of :class:`feedinlib.weather.FeedinWeather`
        objects. Default: None.
    power_plant : dict
        Dictionary containing PV module or wind power plant information to use
        for feed-in calculation. Default: None.
    weather_data_height : dict
        Dictionary containing the height in m the weather data applies to. Must
        have the keys 'pressure', 'temp_air', 'v_wind', and 'Z0'.
        Default: None.

    Returns
    -------
    dict
        Dictionary with keys holding the FeedinWeather object's name and values
        holding the corresponding specific feed-in as pandas.Series.
    """

    if pickle_load:
        data = pickle.load(open(filename, 'rb'))
    else:
        if type == 'wind':
            data = feedin.wind(multi_weather, weather_data_height, power_plant)
        elif type == 'pv':
            data = feedin.pv(multi_weather, **power_plant)
        pickle.dump(data, open(filename, 'wb'))
    return data


