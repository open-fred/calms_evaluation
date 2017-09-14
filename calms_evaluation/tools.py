import pandas as pd
import pickle

import feedin
import weather


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
            data = weather.create_feedinweather_objects_coastdat(
                conn, geom, year)
        elif weather_data == 'merra':
            data = weather.create_feedinweather_objects_merra(conn, filename)
            filename = 'multiweather_merra_' + str(year) + '.p'
        pickle.dump(data, open(filename, 'wb'))
    return data


def get_feedin_data(pickle_load, filename='pickle_dump.p', type=None,
                    multi_weather=None, power_plant=None,
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
