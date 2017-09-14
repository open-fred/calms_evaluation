import os
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import geoplot

from tools import fetch_geometries_from_db


def geo_plot(results_df, conn, legend_label=None, weather_data='coastdat',
             show_plot=True, save_figure=True, filename_plot='geoplot.png',
             save_dir=None, cmap_name='inferno_r', scale_value=None):
    r"""
    Plots results for every FeedinWeather object on a map of Germany.

    Parameters
    ----------
    results_df : pandas.DataFrame
        DataFrame needs to have the name of the FeedinWeather objects as index
        and the values to plot in the column 'results'.
    conn : sqlalchemy connection object
        Use function `connection` from oemof.db to establish database
        connection.
    legend_label : None or String
        Default: None.
    weather_data : String
        Used to retrieve grid geometries from the database. Can be either
        'coastdat' or 'merra'. Default: 'coastdat'.
    show_plot : Boolean
        If True plot is shown. Default: True.
    save_figure : Boolean
        If True plot is stored to directory specified by `save_dir` under
        the name specified by `filename_plot`. Default: True.
    filename_plot : String
        Name the plot is saved under. Default: 'geoplot.png'.
    save_dir : None or String
        Name of directory the plot is saved in. The directory is not created
        automatically but needs to be created manually. If None, the plot is
        saved in the same directory as this file. Default: None.
    cmap_name : String
        Name of the colormap. Default: 'inferno_r'.
    scale_value : None or float
        Value used to scale the results and maximum legend value. If None the
        maximum value of the parameter that is plotted is used. Default: None.

    """
    #ToDo results_df sollte kein df sein? besser series! innerhalb der funktion
    #dann name der series zu results setzen

    fig = plt.figure()

    # retrieve grid geometries from database
    if weather_data == 'coastdat':
        table = 'de_grid'
        schema = 'coastdat'
        geo_col = 'geom'
    elif weather_data == 'merra':
        table = 'merra_grid'
        schema = 'public'
        geo_col = 'geom_grid'
    grid_from_db = {
        'table': table,
        'geo_col': geo_col,
        'id_col': 'gid',
        'schema': schema,
        'simp_tolerance': '0.01',
        'where_col': 'gid',
        'where_cond': '> 0'}
    plot_data = fetch_geometries_from_db(conn, **grid_from_db)
    plot_data['geom'] = geoplot.postgis2shapely(plot_data.geom)
    plot_data = plot_data.set_index('gid')  # set gid as index

    # join geometries with results
    plot_data = plot_data.join(results_df)

    # scale results
    if not scale_value:
        scale_value = max(plot_data['results'].dropna())
    plot_data['results_scaled'] = plot_data['results'] / scale_value

    # plot grid
    grid_plot = geoplot.GeoPlotter(
        geom=plot_data['geom'], bbox=(3, 16, 47, 56),
        data=plot_data['results_scaled'], color='data',
        cmapname=cmap_name)
    grid_plot.plot(edgecolor='')
    grid_plot.draw_legend(legendlabel=legend_label,
        interval=(0, int(scale_value)), integer=True)

    # plot map of Germany with federal states
    germany = {
        'table': 'deu3_21',
        'geo_col': 'geom',
        'id_col': 'region_id',
        'schema': 'deutschland',
        'simp_tolerance': '0.01',
        'where_col': 'region_id',
        'where_cond': '> 0'}
    germany = fetch_geometries_from_db(conn, **germany)
    germany['geom'] = geoplot.postgis2shapely(germany.geom)
    grid_plot.geometries = germany['geom']
    grid_plot.plot(facecolor='', edgecolor='white', linewidth=1)

    plt.tight_layout()
    plt.box(on=None)

    if show_plot:
        plt.show()
    if save_figure:
        if not save_dir:
            save_dir = os.path.dirname(__file__)
        fig.savefig(os.path.abspath(os.path.join(save_dir, filename_plot)))
    plt.close()


def histogram(results_df, legend_label=None, x_label=None, y_label=None,
              x_limit=None, y_limit=None, bin_width=50, tick_freq=100,
              show_plot=True, save_figure=True, filename_plot='histogram.png',
              save_dir=None):
    r"""
    Plots histogram.

    Parameters
    ----------
    results_df : pandas.DataFrame
        DataFrame needs to have the name of the FeedinWeather objects as index
        and the values to plot in the column 'results'.
    legend_label : None or string
        Default: None.
    x_label : None or String
        Default: None.
    y_label : None or String
        Default: None.
    x_limit : None or int
        Maximum value of x-axis. Default: None.
    y_limit : None or int
        Maximum value of y-axis. Default: None.
    bin_width : int
        Default: 50.
    tick_freq : int
        Tick frequency on x-axis. Default: 100.
    show_plot : Boolean
        If True plot is shown. Default: True.
    save_figure : Boolean
        If True plot is stored to directory specified by `save_dir` under
        the name specified by `filename_plot`. Default: True.
    filename_plot : String
        Name the plot is saved under. Default: 'histogram.png'.
    save_dir : None or String
        Name of directory the plot is saved in. The directory is not created
        automatically but needs to be created manually. If None, the plot is
        saved in the same directory as this file. Default: None.

    """

    #ToDo results_df sollte kein df sein? besser series! innerhalb der funktion
    #dann name der series zu results setzen

    data_sorted = np.sort(np.array(results_df['results']))

    fig = plt.figure()
    if x_limit:
        x_max = x_limit
    else:
        x_max = max(data_sorted)
    plt.hist(data_sorted, bins=np.arange(0, x_max + 1, bin_width),
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
        if not save_dir:
            save_dir = os.path.dirname(__file__)
        fig.savefig(os.path.abspath(os.path.join(save_dir, filename_plot)))
    fig.set_tight_layout(True)
    plt.close()


def duration_curve(results_df, legend_label=None, x_label=None, y_label=None,
                   x_limit=None, y_limit=None, show_plot=True,
                   save_figure=True, filename_plot='duration_curve.png',
                   save_dir=None):
    r"""
    Plots duration curve.

    Parameters
    ----------
    results_df : pandas.DataFrame
        DataFrame needs to have the name of the FeedinWeather objects as index
        and the values to plot in the column 'results'.
    legend_label : None or string
        Default: None.
    x_label : None or String
        Default: None.
    y_label : None or String
        Default: None.
    x_limit : None or int
        Maximum value of x-axis. Default: None.
    y_limit : None or int
        Maximum value of y-axis. Default: None.
    show_plot : Boolean
        If True plot is shown. Default: True.
    save_figure : Boolean
        If True plot is stored to directory specified by `save_dir` under
        the name specified by `filename_plot`. Default: True.
    filename_plot : String
        Name the plot is saved under. Default: 'duration_curve.png'.
    save_dir : None or String
        Name of directory the plot is saved in. The directory is not created
        automatically but needs to be created manually. If None, the plot is
        saved in the same directory as this file. Default: None.

    """

    data_sorted = np.sort(np.array(results_df['results']))

    fig = plt.figure()
    plt.plot(data_sorted)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(legend_label)
    if y_limit:
        plt.ylim(ymax=y_limit)
    if x_limit:
        plt.xlim(xmax=x_limit)

    if show_plot:
        plt.show()
    if save_figure:
        if not save_dir:
            save_dir = os.path.dirname(__file__)
        fig.savefig(os.path.abspath(os.path.join(save_dir, filename_plot)))
    fig.set_tight_layout(True)
    plt.close()
