import oemof.db as db
import geoplot
import matplotlib.pyplot as plt
plt.style.use('ggplot')
# import time
from feedinlib import powerplants as plants
from get_from_db import (fetch_shape_germany, get_data, coastdat_geoplot,
                         calculate_avg_wind_speed, calculate_calms,
                         plot_histogram, create_calms_dict, calms_frequency,
                         filter_peaks)

# ----------------------------- Set parameters ------------------------------ #
year = 2011  # 1998 - 2014
# define the power limit for the calms in %
power_limit = [0.03, 0.05, 0.1]  # Must be list or array even if only one entry
load_multi_weather = True  # False if you use a year you haven't dumped yet
load_wind_feedin = True  # False if you use a year you haven't dumped yet
conn = db.connection(section='reiner')

scale_parameter = 1020.0  # If None: standardization with maximum calm lenght
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
# Initialise wind turbine
E126 = plants.WindPowerPlant(**enerconE126)

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
                         filename='multiweather_pickle_{0}.p'.format(year),
                         data_type='multi_weather')
wind_feedin = get_data(power_plant=E126, multi_weather=multi_weather,
                       pickle_load=load_wind_feedin,
                       filename='windfeedin_pickle_{0}.p'.format(year),
                       data_type='wind_feedin')

# -------------------- Calms: Calculations and Geoplots --------------------- #
# Calculate calms
print('Calculating calms...')
for i in range(len(power_limit)):
    # t0 = time.clock()
    print('  ...with power limit: ' + str(int(power_limit[i]*100)) + '%')
    # Get all calms
    calms_dict = create_calms_dict(power_limit[i], wind_feedin)
    # Get all calms with filtered peaks
    calms_dict_filtered = filter_peaks(calms_dict, power_limit[i])
    # Plots
    dict_list = [calms_dict, calms_dict_filtered]
    for k in range(len(dict_list)):
        if k == 0:
            string = ''
        if k == 1:
            string = '_filtered'
        # Geoplot of longest calms of each location
        calms_max, calms_min, calm_lengths = calculate_calms(dict_list[k])
        legend_label = ('Longest calms Germany {0} power limit < {1}%'.format(
            year, int(power_limit[i]*100)) + string)
        coastdat_geoplot(calms_max, conn, show_plot=False,
                         legend_label=legend_label,
                         filename_plot='Longest_calms_{0}_{1}'.format(
                             year, power_limit[i]) + '_std_2011' + string +
                                                     '.png',
                         save_figure=True, save_folder='Plots',
                         scale_parameter=scale_parameter)
        # scaled to maximum of calms
        coastdat_geoplot(calms_max, conn, show_plot=False,
                         legend_label=legend_label,
                         filename_plot='Longest_calms_{0}_{1}'.format(
                             year, power_limit[i]) + string + '.png',
                         save_figure=True, save_folder='Plots')
        # Geoplot of calm lengths > certain calm length (min_lengths)
        min_lengths = [24.0, 7*24.0]
        for j in range(len(min_lengths)):
            frequencies = calms_frequency(calm_lengths, min_lengths[j])
            legend_label = ('Frequency of calms >= ' +
                            '{0} h in {1} power limit < {2}%'.format(
                                int(min_lengths[j]), year,
                                int(power_limit[i] * 100)) + string)
            coastdat_geoplot(frequencies, conn, show_plot=False,
                             legend_label=legend_label,
                             filename_plot='Frequency_{0}h_{1}_{2}'.format(
                                 int(min_lengths[j]), year, power_limit[i]) +
                             string + '.png',
                             save_figure=True, save_folder='Plots')
        # Histogram containing longest calms of each location
        legend_label = 'Calm histogram Germany{0} power limit < {1}%'.format(
            year, int(power_limit[i]*100)) + string
        plot_histogram(calms_max, show_plot=False, legend_label=legend_label,
                       xlabel='Length of calms in h', ylabel='Number of calms',
                       filename_plot='Calm_histogram_{0}_{1}'.format(
                           year, power_limit[i]) + string + '.png',
                       save_figure=True, save_folder='Plots')
        # print(str(time.clock() - t0) + ' seconds since t0')

# --------------------------- Average wind speed ---------------------------- #
print('Calculating average wind speed...')
wind_speed = calculate_avg_wind_speed(multi_weather)
# Geoplot of average wind speed of each location
legend_label = 'Average wind speed_{0}'.format(year)
coastdat_geoplot(wind_speed, conn, show_plot=True, legend_label=legend_label,
                 filename_plot='Average_wind_speed_{0}'.format(year),
                 save_figure=True, save_folder='Plots')

# # ---------------------------- Jahresdauerlinie ----------------------------- #
# # Plot of "Jahresdauerlinie"
# legend_label = 'Annual power duration curve {0}'.format(year)  # TODO: Jahresdauerlinie = Annual power duration curve??
# plot_power_duration_curve(wind_feedin[1114110], show_plot=True,
#                           legend_label=None, xlabel='Hours of the year in h',
#                           ylabel='Normalised power output',
#                           filename_plot='Power_duration_curve_' +
#                           '{0}_1114110'.format(year),
#                           save_figure=True, save_folder='Plots')
