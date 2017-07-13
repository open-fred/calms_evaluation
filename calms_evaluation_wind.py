import oemof.db as db
import pandas as pd
import numpy as np
import geoplot
import matplotlib.pyplot as plt
plt.style.use('ggplot')
from feedinlib import powerplants as plants
from get_from_db import (fetch_shape_germany, get_multiweather,
                         calculate_avg_wind_speed, coastdat_geoplot,
                         calculate_calms, plot_histogram)

# Set parameters
year = 2011  # 1998 - 2014
power_limit = 0.05  # defined the power limit for the calms in %
pickle_load = True  # Set to False if you use a year you haven't dumped yet
conn = db.connection(section='reiner')

# get geometry for Germany
geom = geoplot.postgis2shapely(fetch_shape_germany(conn))
# to plot smaller area
#from shapely import geometry as geopy
#geom = [geopy.Polygon(
    #[(12.2, 52.2), (12.2, 51.6), (13.2, 51.6), (13.2, 52.2)])]

# get multiweather
print('Collecting weather objects...')
multi_weather = get_multiweather(conn, year=year,
                                 geom=geom[0],
                                 pickle_load=pickle_load,
                                 filename='multiweather_pickle_' +
                                 '{0}.p'.format(year))
print('Calculating calms...')
# Set parameters for calms
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
# Calculate calms
calms_1 = calculate_calms(multi_weather, E126, power_limit)

# calculate average wind speed
wind_speed = calculate_avg_wind_speed(multi_weather)

# plots
print('Creating plots...')
legend_label = 'Average wind speed'
coastdat_geoplot(wind_speed, conn, show_plot=True, legend_label=legend_label,
                 filename_plot='Average_wind_speed', save_figure=True)
legend_label = 'Longest calms Germany {0}'.format(year)
coastdat_geoplot(calms_1, conn, show_plot=True, legend_label=legend_label,
                 filename_plot='Longest_calms_{0}'.format(year),
                 save_figure=True, scale_parameter=1020.0)
legend_label = 'Calm histogram Germany{0}'.format(year)
plot_histogram(calms_1, show_plot=True, legend_label=legend_label,
               xlabel='Length of calms in h', ylabel='Number of calms',
               filename_plot='Calm_histogram_{0}'.format(year),
               save_figure=True)
