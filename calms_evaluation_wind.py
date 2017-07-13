import oemof.db as db
import pandas as pd
import numpy as np
import geoplot
import matplotlib.pyplot as plt
plt.style.use('ggplot')
from feedinlib import powerplants as plants
from get_from_db import (fetch_shape_germany, get_data,
                         calculate_avg_wind_speed, coastdat_geoplot,
                         calculate_calms, plot_histogram)

# ----------------------------- Set parameters ------------------------------ #
year = 2011  # 1998 - 2014
power_limit = 0.05  # defined the power limit for the calms in %
load_multi_weather = True  # False if you use a year you haven't dumped yet
load_wind_feedin = False  # False if you use a year you haven't dumped yet
conn = db.connection(section='reiner')

normalise = 1020.0  # If None: normalisation with maximum calm lenght
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

# ------------------------------ Calculations ------------------------------- #
# Calculate calms
print('Calculating calms...')
calms_max, calms_min = calculate_calms(multi_weather, E126, power_limit,
                                       wind_feedin)

# Calculate average wind speed
wind_speed = calculate_avg_wind_speed(multi_weather)

# ---------------------------------- Plots ---------------------------------- #

print('Creating plots...')
# Geoplot of average wind speed of each location
legend_label = 'Average wind speed_{0}'.format(year)
coastdat_geoplot(wind_speed, conn, show_plot=True, legend_label=legend_label,
                 filename_plot='Average_wind_speed_{0}'.format(year),
                 save_figure=True)
# Geoplot of longest calms of each location
legend_label = 'Longest calms Germany {0}'.format(year)
coastdat_geoplot(calms_max, conn, show_plot=True, legend_label=legend_label,
                 filename_plot='Longest_calms_{0}'.format(year),
                 save_figure=True, scale_parameter=1020.0)
# Histogram containing longest calms of each location
legend_label = 'Calm histogram Germany{0}'.format(year)
plot_histogram(calms_max, show_plot=True, legend_label=legend_label,
               xlabel='Length of calms in h', ylabel='Number of calms',
               filename_plot='Calm_histogram_{0}'.format(year),
               save_figure=True)
