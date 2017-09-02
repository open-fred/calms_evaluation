import os
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import geoplot

from tools import fetch_geometries


def geo_plot(results_df, conn, weather_data='coastdat', show_plot=True,
                    legend_label=None, save_figure=True, save_folder='Plots',
                    cmapname='inferno_r', scale_parameter=None,
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

    if show_plot:
        plt.show()
    if save_figure:
        fig.savefig(os.path.abspath(os.path.join(
            os.path.dirname(__file__), '..', save_folder, filename_plot)))
    plt.close()
    return


def histogram(calms, show_plot=True, legend_label=None, x_label=None,
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


# def power_duration_curve(wind_feedin, show_plot=True, legend_label=None,
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