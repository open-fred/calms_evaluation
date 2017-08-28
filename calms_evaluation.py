import oemof.db as db
import geoplot
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import numpy as np
import pandas as pd
# import time
import pickle
from feedinlib import powerplants as plants
from get_from_db import (fetch_shape_germany, get_data, weather_geoplot,
                         calculate_avg_wind_speed, calculate_calms,
                         plot_histogram, create_calms_dict, calms_frequency,
                         filter_peaks)

# ----------------------------- Set parameters ------------------------------ #
year = 2011  # 1998 - 2014
weather_data = 'coastdat'  # 'coastdat' or 'merra'
power_limit = [0.03, 0.05, 0.1]  # Must be list or array even if only one entry
load_multi_weather = True  # False if you use a year you haven't dumped yet
load_wind_feedin = True  # False if you use a year you haven't dumped yet
load_pv_feedin = False  # False if you use a year you haven't dumped yet
calms_filtered_load = True  # False is you haven't dumped the dictionary yet
conn = db.connection(section='reiner')
show_plot = False
save_figure = True
energy_source = 'Wind'  # 'Wind', 'PV' or 'Wind_PV'
# Filter or don't filter peaks (or both)
filter = [
    'unfiltered',  # always calculated, but only plotted if not uncommented
    'filtered'  # only calculated and plottet if not uncommented
]

# ----------------------- Plots and their parameters ------------------------ #
# The following plots are created:
# Geoplots (and parameters)
geoplots = [
    'longest_calms',
    'frequency'
]
scale_parameter = None  # If None: standardization with maximum calm length
save_folder1 = 'Plots'
cmapname = 'inferno_r'
min_lengths = [24.0, 48.0, 7*24.0]  # Minimum calm lengths for frequency plot

# Histograms (and parameters)
histograms = [
    'longest_calms',
    # 'all_calms'
]
x_label = 'Length of calms in h'  # None or string
y_label = 'Number of calms'  # None or string
save_folder2 = save_folder1
y_limit = 500  # None or integer
x_limit = 1200  # None or integer
bin_width = 50  # Integer
tick_freq = 200  # Frequency of x-ticks

# Others
others = [
    'average_wind_speed',
    # 'average_irradiance'  # not implemented yet
]
save_folder3 = save_folder1

# ---------------------- Weather and power plant data ----------------------- #
# Specification of the weather data set CoastDat2
coastDat2 = {
    'dhi': 0,
    'dirhi': 0,
    'pressure': 0,
    'temp_air': 2,
    'v_wind': 10,
    'Z0': 0}

# Specification of the wind turbines
enerconE126 = {
    'h_hub': 135,
    'd_rotor': 127,
    'wind_conv_type': 'ENERCON E 126 7500',
    'data_height': coastDat2}

# Specification of the pv module
advent210 = {
    'module_name': 'Advent_Solar_Ventura_210___2008_',
    'azimuth': 180,
    'tilt': 60,
    'albedo': 0.2}

# Get geometry for Germany for geoplot
geom = geoplot.postgis2shapely(fetch_shape_germany(conn))
# to plot smaller area
#from shapely import geometry as geopy
#geom = [geopy.Polygon(
    #[(12.2, 52.2), (12.2, 51.6), (13.2, 51.6), (13.2, 52.2)])]

# -------------------------- Get weather objects ---------------------------- #
print(' ')
print('Collecting weather objects...')
multi_weather = get_data(conn=conn, year=year, geom=geom[0],
                         pickle_load=load_multi_weather,
                         filename='multiweather_{0}_{1}.p'.format(
                             weather_data, year),
                         data_type='multi_weather_{0}.p'.format(weather_data))

# ------------------------------ Feedin data -------------------------------- #
if (energy_source == 'Wind' or energy_source == 'Wind_PV'):
    turbine = plants.WindPowerPlant(**enerconE126)
    feedin = get_data(power_plant=turbine, multi_weather=multi_weather,
                      pickle_load=load_wind_feedin,
                      filename='windfeedin_{0}_{1}.p'.format(
                          weather_data, year),
                      data_type='wind_feedin')
if (energy_source == 'PV' or energy_source == 'Wind_PV'):
    module = plants.Photovoltaic(**advent210)
    feedin = get_data(power_plant=module, multi_weather=multi_weather,
                      pickle_load=load_pv_feedin,
                      filename='pv_feedin__{0}_{1}.p'.format(
                          weather_data, year),
                      data_type='pv_feedin')
# TODO: total sum of feedins for PV + Wind (feedin: Dictionary, keys: gids)
# -------------------- Calms: Calculations and Geoplots --------------------- #
# Calculate calms
print('Calculating calms...')
# t0 = time.clock()
for i in range(len(power_limit)):
    print('  ...with power limit: ' + str(int(power_limit[i]*100)) + '%')
    # Get all calms
    calms_dict = create_calms_dict(power_limit[i], feedin)
    dict_list = []
    if 'unfiltered' in filter:
        dict_list.append(calms_dict)
    if 'filtered' in filter:
        # Get all calms with filtered peaks
        filename = 'calms_dict_filtered_pickle_{0}_{1}_{2}_{3}.p'.format(
            weather_data, year, energy_source, power_limit[i])
        if calms_filtered_load:
            calms_dict_filtered = pickle.load(open(filename, 'rb'))
        else:
            calms_dict_filtered = filter_peaks(calms_dict, power_limit[i])
            pickle.dump(calms_dict_filtered, open(filename, 'wb'))
        dict_list.append(calms_dict_filtered)
    # Plots
    for k in range(len(dict_list)):
        if (k == 0 and 'unfiltered' in filter):
            string = ''
        if (k == 1 or (k == 0 and 'unfiltered' not in filter)):
            string = 'filtered'
        calms_max, calms_min, calm_lengths = calculate_calms(dict_list[k])
        if 'longest_calms' in geoplots:
            # Geoplot of longest calms of each location
            legend_label = ('Longest calms in hours Germany ' +
                            '{0} power limit < {1}% {2} {3} {4}'.format(
                                year, int(power_limit[i]*100), energy_source,
                                string, weather_data))
            weather_geoplot(calms_max, conn, weather_data, show_plot,
                            legend_label, save_figure, save_folder1, cmapname,
                            scale_parameter,
                            filename_plot='Longest_calms_' +
                                           '{0}_{1}_{2}_{3}_{4}.png'.format(
                                               energy_source, weather_data,
                                               year, power_limit[i], string))
        if 'frequency' in geoplots:
            # Creates Plot only for unfiltered calms
            if (k == 0 and 'unfiltered' in filter):
                # Geoplot of calm lengths > certain calm length (min_lengths)
                for j in range(len(min_lengths)):
                    frequencies = calms_frequency(calm_lengths, min_lengths[j])
                    legend_label = (
                        'Frequency of calms >= ' +
                        '{0} h in {1} power limit < {2}% {3} {4}'.format(
                            int(min_lengths[j]), year,
                            int(power_limit[i] * 100), energy_source,
                            weather_data))
                    weather_geoplot(frequencies, conn, weather_data, show_plot,
                                    legend_label, save_figure, save_folder1,
                                    cmapname, scale_parameter,
                                    filename_plot=
                                    'Frequency_{0}_{1}_{2}h_{3}_{4}.png'.format(
                                         energy_source, weather_data,
                                         int(min_lengths[j]), year,
                                         power_limit[i]))
        if 'longest_calms' in histograms:
            # Histogram containing longest calms of each location
            legend_label = ('Longest calms Germany ' +
                            '{0} power limit < {1}% {2} {3} {4}'.format(
                                year, int(power_limit[i]*100), energy_source,
                                string, weather_data))
            plot_histogram(calms_max, show_plot, legend_label, x_label,
                           y_label, save_folder2, save_figure, y_limit,
                           x_limit, bin_width, tick_freq,
                           filename_plot='Histogram_longest_calms_' +
                                         '_{0}_{1}_{2}_{3}_{4}.png'.format(
                                             energy_source, weather_data, year,
                                             power_limit[i], string))
        if 'all_calms' in histograms:
            # Histogram containing all calms of all location
            calm_arr = np.array([])
            for key in calm_lengths:
                calm_arr = np.append(calm_arr, calm_lengths[key])
            calm_df = pd.DataFrame(data=calm_arr, columns=['results'])
            legend_label = ('Calms Germany ' +
                            '{0} power limit < {1}% {2} {3} {4}'.format(
                                year, int(power_limit[i] * 100), energy_source,
                                string, weather_data))
            plot_histogram(calm_df, show_plot, legend_label, x_label, y_label,
                           save_folder2, save_figure, y_limit, x_limit,
                           bin_width, tick_freq,
                           filename_plot='Histogram_calms_' +
                                         '_{0}_{1}_{2}_{3}_{4}.png'.format(
                                             energy_source, weather_data, year,
                                             power_limit[i], string))
# print(str(time.clock() - t0) + ' seconds since t0')

# --------------------------- Average wind speed ---------------------------- #
if 'average_wind_speed' in others:
    print('Calculating average wind speed...')
    wind_speed = calculate_avg_wind_speed(multi_weather)
    # Geoplot of average wind speed of each location
    legend_label = 'Average wind speed {0} {1}'.format(year, weather_data)
    weather_geoplot(wind_speed, conn, weather_data, show_plot, legend_label,
                    save_figure, save_folder3, cmapname,
                    filename_plot='Average_wind_speed_{0}_{1}'.format(
                        year, weather_data))

# # ---------------------------- Jahresdauerlinie ----------------------------- #
# # Plot of "Jahresdauerlinie"
# legend_label = 'Annual power duration curve {0}'.format(year)  # TODO: Jahresdauerlinie = Annual power duration curve??
# plot_power_duration_curve(feedin[1114110], show_plot=True,
#                           legend_label=None, xlabel='Hours of the year in h',
#                           ylabel='Normalised power output',
#                           filename_plot='Power_duration_curve_' +
#                           '{0}_1114110'.format(year),
#                           save_figure=True, save_folder='Plots')
