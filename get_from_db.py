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


def get_data(conn=None, power_plant=None, multi_weather=None, year=None,
             geom=None, pickle_load=True, filename='pickle_dump.p',
             data_type='multi_weather'):
    if not pickle_load:
        if data_type == 'multi_weather':
            data = coastdat.get_weather(conn, geom, year)
        if data_type == 'wind_feedin':
            data = {}
            for i in range(len(multi_weather)):
                data[multi_weather[i].name] = power_plant.feedin(
                    weather=multi_weather[i], installed_capacity=1)
        if data_type == 'pv_feedin':
            data = {}
            for i in range(len(multi_weather)):
                data[multi_weather[i].name] = power_plant.feedin(
                    weather=multi_weather[i], peak_power=1)
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
        feedin_arr = np.array(df['feedin_wind_pp'])
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


def coastdat_geoplot(results_df, conn, show_plot=True, legend_label=None,
                     save_figure=True, save_folder='Plots',
                     cmapname='inferno_r', scale_parameter=None,
                     filename_plot='plot.png'):
    """
    results_df should have the coastdat region gid as index and the values
    that are plotted (average wind speed, calm length, etc.) in the column
    'results'
    """
    fig = plt.figure()
    # plot coastdat cells with results
    coastdat_de = {
        'table': 'de_grid',
        'geo_col': 'geom',
        'id_col': 'gid',
        'schema': 'coastdat',
        'simp_tolerance': '0.01',
        'where_col': 'gid',
        'where_cond': '> 0'
    }
    coastdat_de = fetch_geometries(conn, **coastdat_de)
    coastdat_de['geom'] = geoplot.postgis2shapely(coastdat_de.geom)
    coastdat_de = coastdat_de.set_index('gid')  # set gid as index
    coastdat_de = coastdat_de.join(results_df)  # join results
    # scale results
    if not scale_parameter:
        scale_parameter = max(coastdat_de['results'].dropna())
    coastdat_de['results_scaled'] = coastdat_de['results'] / scale_parameter
    coastdat_plot = geoplot.GeoPlotter(
        geom=coastdat_de['geom'], bbox=(3, 16, 47, 56),
        data=coastdat_de['results_scaled'], color='data', cmapname=cmapname)
    coastdat_plot.plot(edgecolor='')
    coastdat_plot.draw_legend(legendlabel=legend_label,
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

    coastdat_plot.geometries = germany['geom']
    coastdat_plot.plot(facecolor='', edgecolor='white', linewidth=1)

    plt.tight_layout()
    plt.box(on=None)

    if show_plot:
        plt.show()
    if save_figure:
        fig.savefig(os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', save_folder, filename_plot)))
    plt.close()
    return


def plot_histogram(calms, show_plot=True, legend_label=None, x_label=None,
                   y_label=None, save_folder='Plots', save_figure=True,
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
