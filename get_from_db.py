import oemof.db as db
from oemof.db import coastdat
import pandas as pd
import numpy as np
import geoplot
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import pickle
import os


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


def get_multiweather(conn, year=None, geom=None, pickle_load=True,
                     filename='multiweather_pickle.p'):
    if not pickle_load:
        multi_weather = coastdat.get_weather(conn, geom, year)
        pickle.dump(multi_weather, open(filename, 'wb'))
    if pickle_load:
        multi_weather = pickle.load(open(filename, 'rb'))
    return multi_weather


def calculate_avg_wind_speed(multi_weather):
    avg_wind_speed = {}
    for i in range(len(multi_weather)):
        avg_wind_speed[multi_weather[i].name] = np.mean(
            multi_weather[i].data.v_wind)
    avg_wind_speed = pd.DataFrame(data=avg_wind_speed,
                                  index=['results']).transpose()
    return avg_wind_speed


def calculate_calms(multi_weather, power_plant, power_limit):
    """
    Collecting calm vectors in dictionary vector_coll
    Loop over all weather objects to find the longest calms for each region.
    Returns DataFrame.
    """
    vector_coll = {}
    calms_1 = {}
    for i in range(len(multi_weather)):
        wind_feedin = power_plant.feedin(weather=multi_weather[i],
                                         installed_capacity=1)
        calm, = np.where(wind_feedin < power_limit)  # defines the calm
        # find all calm periods
        vector_coll = np.split(calm, np.where(np.diff(calm) != 1)[0] + 1)
        # find the longest calm from all periods
        calm = len(max(vector_coll, key=len))
        calms_1[multi_weather[i].name] = calm
    # Create DataFrames
    calms_1 = pd.DataFrame(data=calms_1, index=['results']).transpose()
    return calms_1


def coastdat_geoplot(results_df, conn, show_plot=True, legend_label=None,
                     filename_plot='plot.png', save_figure=True,
                     cmapname='inferno', scale_parameter=None):
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
        interval=(0, int(scale_parameter)))

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
            os.path.dirname(__file__), '..', 'Plots', filename_plot)))
    return


def plot_histogram(calms, show_plot=True, legend_label=None, xlabel=None,
                   ylabel=None, filename_plot='plot_histogram.png',
                   save_figure=True):
    """ calms should have the coastdat region gid as index and the values
    that are plotted in the column 'results'.
    Histogram contains longest calms of each location.
    """
    # sort calms
    calms_sorted = np.sort(np.array(calms['results']))
    # plot
    fig = plt.figure()
    plt.hist(calms_sorted, normed=False, range=(calms_sorted.min(),
                                                calms_sorted.max()))
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(legend_label)
    if show_plot:
        plt.show()
    if save_figure:
        fig.savefig(os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', 'Plots', filename_plot)))
    fig.set_tight_layout(True)
    plt.close()

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
    multi_weather = get_multiweather(conn, year=year,
                                     geom=geom[0],
                                     pickle_load=pickle_load,
                                     filename='multiweather_pickle.p')
    # calculate average wind speed
    calc = calculate_avg_wind_speed(multi_weather)

    # plot
    coastdat_geoplot(calc, conn, show_plot=True, legend_label=legend_label,
                     filename_plot='plot.png', save_figure=True)
