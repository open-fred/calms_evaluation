import oemof.db as db
from oemof.db import coastdat
import pandas as pd
import numpy as np
import geoplot
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import pickle
import os
import copy
from feedinlib.weather import FeedinWeather
from shapely.geometry import Point
import dateutil.parser
import pvlib
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain


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


def calculate_pv_feedin(multi_weather, module_name, inverter_name,
                        azimuth, tilt, albedo):
    print('Calculating PV feedin...')
    pv_feedin = {}

    smodule = {
        'module_parameters': pvlib.pvsystem.retrieve_sam('sandiamod')[
            module_name],
        'inverter_parameters': pvlib.pvsystem.retrieve_sam('sandiainverter')[
            inverter_name],
        'surface_azimuth': azimuth,
        'surface_tilt': tilt,
        'albedo': albedo}

    number_of_weather_points = len(multi_weather)
    for i in range(len(multi_weather)):
        if i % 50 == 0:
            print('  ...weather object {0} from {1}'.format(
                str(i), str(number_of_weather_points)))
        location = {'latitude': multi_weather[i].latitude,
                    'longitude': multi_weather[i].longitude}

        weather = copy.deepcopy(multi_weather[i].data)
        weather['ghi'] = weather['dhi'] + weather['dirhi']
        weather['temp_air'] = weather.temp_air - 273.15
        weather.rename(columns={'v_wind': 'wind_speed'},
                       inplace=True)

        p_peak = (
            smodule['module_parameters'].Impo *
            smodule['module_parameters'].Vmpo)

        # pvlib's ModelChain
        mc = ModelChain(PVSystem(**smodule), Location(**location))
        mc.complete_irradiance(times=weather.index, weather=weather)
        mc.run_model(times=weather.index, weather=weather)

        feedin_scaled = mc.dc.p_mp.fillna(0) / p_peak
        feedin_scaled.name = 'feedin'
        pv_feedin[multi_weather[i].name] = feedin_scaled

    return pv_feedin


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
            data = calculate_pv_feedin(multi_weather, **power_plant)
        pickle.dump(data, open(filename, 'wb'))
    if pickle_load:
        data = pickle.load(open(filename, 'rb'))
    return data


def calculate_avg_wind_speed(multi_weather):
    avg_wind_speed = {}
    for i in range(len(multi_weather)):
        avg_wind_speed[multi_weather[i].name] = np.mean(
            multi_weather[i].data.v_wind)
    avg_wind_speed = pd.DataFrame(data=avg_wind_speed,
                                  index=['results']).transpose()
    return avg_wind_speed


def create_calms_dict(power_limit, wind_feedin):
    """
    Creates a Dictonary containing DataFrames for all locations (keys: gids of
    locations) with the wind feedin time series (column 'feedin_wind_pp') and
    information about calms (column 'calm' - calm: value of wind feedin,
    no calm: 'no_calm').
    """
    calms_dict = {}
    for key in wind_feedin:
        feedin = pd.DataFrame(data=wind_feedin[key])
        # Find calms
        calms = feedin.where(feedin < power_limit, other='no_calm')
        calms.columns = ['calm']
        calms_dict[key] = pd.concat([feedin, calms],
                                    axis=1)  # brings columns to the same level
    return calms_dict


def calculate_calms(calms_dict):
    """
    Returns the calm lengths of all the calms at each location and finds the
    longest and shortest calm from all the calms at each location.

    Returns
    -------
    calms_max : DataFrame
        indices: gids of location, data: longest calm of location.
    calms_min : DataFrame
        indices: gids of location, data: shortest calm of location.
    calm_lengths : Dictionary
        keys: gids of weather location, data: array
        Length of the single calms for each location.
    """
    calms_max, calms_min, calm_lengths = {}, {}, {}
    for key in calms_dict:
        df = calms_dict[key]
        # Find calm periods
        calms, = np.where(df['calm'] != 'no_calm')
        calm_arrays = np.split(calms, np.where(np.diff(calms) != 1)[0] + 1)
        # Write the calm lengths into array of dictionary calm_lengths
        calm_lengths[key] = np.array([len(calm_arrays[i])
                                      for i in range(len(calm_arrays))])
        # Find the longest and shortest calm from all periods
        maximum = max(calm_lengths[key])
        calms_max[key] = maximum
        minimum = min(calm_lengths[key])
        calms_min[key] = minimum
    # Create DataFrame
    calms_max = pd.DataFrame(data=calms_max, index=['results']).transpose()
    calms_min = pd.DataFrame(data=calms_min, index=['results']).transpose()
    return calms_max, calms_min, calm_lengths


def calms_frequency(calm_lengths, min_length):
    """
    Finds the frequency of calms with length >= min_length for each
    location.
    """
    calms_freq = {}
    for key in calm_lengths:
        calms_freq[key] = np.compress((calm_lengths[key] >= min_length),
                                      calm_lengths[key]).size
    calms_freq = pd.DataFrame(data=calms_freq, index=['results']).transpose()
    return calms_freq


def filter_peaks(calms_dict, power_limit):
    """
    Filteres the peaks from the calms using a running average.
    """
    # TODO: Could be run a second time with the camls_dict_filtered to filter possilble peaks again
    calms_dict_filtered = copy.deepcopy(calms_dict)
    for key in calms_dict_filtered:
        df = calms_dict_filtered[key]
        # Find calm periods
        calms, = np.where(df['calm'] != 'no_calm')
        calm_arrays = np.split(calms, np.where(np.diff(calms) != 1)[0] + 1)
        # Filter out peaks
        feedin_arr = np.array(df['feedin'])
        calm_arr = np.array(df['calm'])
        i = 0
        while i <= (len(calm_arrays) - 1):
            j = i + 1
            if j > (len(calm_arrays) - 1):
                break
            while (sum(feedin_arr[calm_arrays[i][0]:calm_arrays[j][-1] + 1]) /
                   len(feedin_arr[calm_arrays[i][0]:calm_arrays[j][-1] + 1])
                   < power_limit):
                j = j + 1
                if j > (len(calm_arrays) - 1):
                    break
            calm_arr[calm_arrays[i][0]:calm_arrays[j-1][-1] + 1] = feedin_arr[
                calm_arrays[i][0]:calm_arrays[j-1][-1] + 1]
            i = j
        df2 = pd.DataFrame(data=calm_arr, columns=['calm2'], index=df.index)
        df_final = pd.concat([df, df2], axis=1)
        df_final = df_final.drop('calm', axis=1)
        df_final.columns = ['feedin_wind_pp', 'calm']
        calms_dict_filtered[key] = df_final
    return calms_dict_filtered


def weather_geoplot(results_df, conn, save_folder, scale_parameter, weather_data='coastdat', show_plot=True,
                    legend_label=None, save_figure=True,
                    cmapname='inferno_r',
                    filename_plot='plot.png'):
    """
    results_df should have the region gid as index and the values
    that are plotted (average wind speed, calm length, etc.) in the column
    'results'
    """
    fig = plt.figure()
    # plot weather data cells with results
    if weather_data == 'coastdat':
        table = 'de_grid'
        schema = 'coastdat'
        geo_col = 'geom'
    elif weather_data == 'merra':
        table = 'merra_grid'
        schema = 'public'
        geo_col = 'geom_grid'

    weather_plot_data = {
        'table': table,
        'geo_col': geo_col,
        'id_col': 'gid',
        'schema': schema,
        'simp_tolerance': '0.01',
        'where_col': 'gid',
        'where_cond': '> 0'
    }
    weather_plot_data = fetch_geometries(conn, **weather_plot_data)
    weather_plot_data['geom'] = geoplot.postgis2shapely(weather_plot_data.geom)
    weather_plot_data = weather_plot_data.set_index('gid')  # set gid as index
    weather_plot_data = weather_plot_data.join(results_df)  # join results
    # scale results
    if not scale_parameter:
        scale_parameter = max(weather_plot_data['results'].dropna())

    weather_plot_data['results_scaled'] = (weather_plot_data['results'] /
                                           scale_parameter)
    weather_plot = geoplot.GeoPlotter(
        geom=weather_plot_data['geom'], bbox=(3, 16, 47, 56),
        data=weather_plot_data['results_scaled'], color='data',
        cmapname=cmapname)
    weather_plot.plot(edgecolor='')
    weather_plot.draw_legend(legendlabel=legend_label,
        interval=(0, int(scale_parameter)), integer=True)

    # plot Germany with regions
    germany = {
        'table': 'deu3_21',
        'geo_col': 'geom',
        'id_col': 'region_id',
        'schema': 'deutschland',
        'simp_tolerance': '0.01',
        'where_col': 'region_id',
        'where_cond': '> 0'}
    germany = fetch_geometries(conn, **germany)
    germany['geom'] = geoplot.postgis2shapely(germany.geom)

    weather_plot.geometries = germany['geom']
    weather_plot.plot(facecolor='', edgecolor='white', linewidth=1)

    plt.tight_layout()
    plt.box(on=None)

    # if show_plot:
        # plt.show()
    if save_figure:
        fig.savefig(os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', save_folder, filename_plot)))
    plt.close()
    return


def plot_histogram(calms, save_folder, show_plot=True, legend_label=None, x_label=None,
                   y_label=None, save_figure=True,
                   y_limit=None, x_limit=None, bin_width=50, tick_freq=100,
                   filename_plot='plot_histogram.png'):
    """
    calms should have the coastdat region gid as index and the values
    that are plotted in the column 'results'.
    Histogram contains longest calms of each location.
    """
    # sort calms
    calms_sorted = np.sort(np.array(calms['results']))
    # plot
    fig = plt.figure()
    if x_limit:
        x_max = x_limit
    else:
        x_max = max(calms_sorted)
    plt.hist(calms_sorted, bins=np.arange(0, x_max + 1, bin_width),
             normed=False)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(np.arange(0, x_max + 1, tick_freq))
    if y_limit:
        plt.ylim(ymax=y_limit)
    if x_limit:
        plt.xlim(xmax=x_limit)
    plt.title(legend_label)
    if show_plot:
        plt.show()
    if save_figure:
        fig.savefig(os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', save_folder, filename_plot)))
    fig.set_tight_layout(True)
    plt.close()


# def plot_power_duration_curve(wind_feedin, show_plot=True, legend_label=None,
#                               xlabel=None, ylabel=None,
#                               filename_plot='plot_annual_curve.png',
#     save_folder = 'Plots',
#                               save_figure=True):
#     """
#     Plots the annual power duration curve(s) (Jahresdauerlinie) of wind feedin
#     time series.
#     """
# #    for i in range(len(wind_feedin)):
#     # Sort feedin
#     feedin_sorted = np.sort(np.array(wind_feedin))
#     # Plot
#     fig = plt.figure()
#     plt.plot(feedin_sorted)
#     plt.xlabel(xlabel)
#     plt.ylabel(ylabel)
#     plt.title(legend_label)
#     plt.ylim(ymax=0.1)
#     plt.xlim(xmax=2500)
#     if show_plot:
#         plt.show()
#     if save_figure:
#         fig.savefig(os.path.abspath(os.path.join(
#             os.path.dirname(__file__), '..', save_folder, filename_plot)))
#     fig.set_tight_layout(True)
#     plt.close()

if __name__ == "__main__":

    year = 2011
    conn = db.connection(section='reiner')
    legend_label = 'Average wind speed'
    pickle_load = False
    # get geometry for Germany
    geom = geoplot.postgis2shapely(fetch_shape_germany(conn))
    # to plot smaller area
    #from shapely import geometry as geopy
    #geom = [geopy.Polygon(
        #[(12.2, 52.2), (12.2, 51.6), (13.2, 51.6), (13.2, 52.2)])]
    # get multiweather
    multi_weather = get_data(conn, year=year, geom=geom[0],
                             pickle_load=pickle_load,
                             filename='multiweather_pickle.p')
    # calculate average wind speed
    calc = calculate_avg_wind_speed(multi_weather)

    # plot
    coastdat_geoplot(calc, conn, show_plot=True, legend_label=legend_label,
                     filename_plot='plot.png', save_figure=True)
