import matplotlib
matplotlib.use('pdf')  # generate PDF output by default
import matplotlib.pyplot as plt
import oemof.db as db
#from shapely import geometry as geopy
#from shapely.geometry import Polygon
from oemof.db import coastdat
import pandas as pd
import numpy as np
import geoplot
plt.style.use('ggplot')
from shapely.geometry import shape
import fiona
from feedinlib import powerplants as plants
import pickle
#from shapely.wkt import loads as load_wkt
from geopy.geocoders import Nominatim
# from progressbar import ProgressBar
import os


def fetch_geometries(union=False, **kwargs):
    """Reads the geometry and the id of all given tables and writes it to
     the 'geom'-key of each branch of the data tree.
    """
    if not union:
        sql_str = '''
            SELECT {id_col}, ST_AsText(
                ST_SIMPLIFY({geo_col}, {simp_tolerance})) geom
            FROM {schema}.{table}
            WHERE "{where_col}" {where_cond}
            ORDER BY {id_col} DESC;'''
    else:
        sql_str = '''
            SELECT ST_AsText(
                ST_SIMPLIFY(st_union({geo_col}), {simp_tolerance})) as geom
            FROM {schema}.{table}
            WHERE "{where_col}" {where_cond};'''
    db_string = sql_str.format(**kwargs)
    results = db.connection(section='reiners_db').execute(db_string)
    cols = results.keys()
    return pd.DataFrame(results.fetchall(), columns=cols)

print('please wait...')

germany_u = {
    'table': 'deu3_21',
    'geo_col': 'geom',
    'id_col': 'region_id',
    'schema': 'deutschland',
    'simp_tolerance': '0.01',
    'where_col': 'region_id',
    'where_cond': '> 0',
    }

year = 2011

# Connection to the weather data
print('collecting weather objects...')
conn = db.connection(section='reiners_db')
germany_u = fetch_geometries(union=True, **germany_u)
germany_u['geom'] = geoplot.postgis2shapely(germany_u.geom)

# Fiona read shape file to define the area to analyse
c = fiona.open(os.path.join(os.path.dirname(__file__),
                            'germany_and_offshore/germany_and_offshore.shp'))
pol = c.next()
geom = shape(pol['geometry'])


####--------PICKLE---Use pickle to save or load the weather objects---------###

#multi_weather = pickle.load(open('multi_weather_save.p', 'rb'))
multi_weather = coastdat.get_weather(conn, germany_u['geom'][0], year)
my_weather = multi_weather[0]
pickle.dump(multi_weather, open('multi_weather_save.p', 'wb'))

##########-------feedinlib Components--------------------------################

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
    'tilt': 30,
    'albedo': 0.2}

# Definition of the power plants
E126_power_plant = plants.WindPowerPlant(**enerconE126)
advent_module = plants.Photovoltaic(**advent210)


########----------------Calculating calms----------------------#############

print('calculating calms...')
vector_coll = {}
calm_list = []

power_limit = 0.05  # defined the power limit for the calms in %

# Collecting calm vectors in dictionary vector_coll
# Loop over 792 weather objects to find the longest calms for each
for i in range(len(multi_weather)):
    wind_feedin = E126_power_plant.feedin(weather=multi_weather[i],
                                          installed_capacity=1)
    calm, = np.where(wind_feedin < power_limit)  # defines the calm
    # find all calm periods
    vector_coll = np.split(calm, np.where(np.diff(calm) != 1)[0] + 1)
    vc = vector_coll
    calm = len(max(vc, key=len))  # find the longest calm from all periods
    calm_list = np.append(calm_list, calm)  # append it to a copy of the list
    calm_list2 = (calm_list) / (calm_list.max(axis=0))  # normalise calms
    calm_list3 = np.sort(calm_list)  # sort calms
#    print('done_' + str(i+1), '/792')

# print results
x = np.amax(calm_list)  # maximum of the longest
y = np.amin(calm_list)  # minimum of the longest
z = sum(calm_list) / 792  # average of the longest
print('-> average calm lenght', z, 'hours')
print()
print('-> longest calm:', x, 'hours')
print('-> shortest calm:', y, 'hours')
print()

# Histogram, contains longest calms of each location
figure = plt.figure()
plt.hist(calm_list3, normed=False, range=(calm_list.min(),
                                          calm_list.max()))
plt.xlabel('length of calms in h')
plt.ylabel('number of calms')
plt.title('Calm histogram Germany{0}'.format(year))
figure.savefig(os.path.join('Plots/histograms',
                            'calm_histogram_{0}'.format(year)))
figure.set_tight_layout(True)
plt.close()

coastdat_de = {
    'table': 'de_grid',
    'geo_col': 'geom',
    'id_col': 'gid',
    'schema': 'coastdat',
    'simp_tolerance': '0.01',
    'where_col': 'gid',
    'where_cond': '> 0'
    }
germany = {
    'table': 'deu3_21',
    'geo_col': 'geom',
    'id_col': 'region_id',
    'schema': 'deutschland',
    'simp_tolerance': '0.01',
    'where_col': 'region_id',
    'where_cond': '> 0',
    }

coastdat_de = fetch_geometries(**coastdat_de)
coastdat_de['geom'] = geoplot.postgis2shapely(coastdat_de.geom)
germany = fetch_geometries(**germany)
germany['geom'] = geoplot.postgis2shapely(germany.geom)


# Build dataframe including the calms and its location (geom)

print('building Dataframe...')
print()

# calm_list2 -> normalised calms
d = {'id': np.arange(len(multi_weather)), 'calms': calm_list2}
x = coastdat_de['geom']
df = pd.DataFrame(data=d)
df2 = pd.DataFrame(data=x, columns=['geom'])
df3 = pd.concat([df, df2],
                axis=1)  # axis=1 brings booth colums to the same level
df5 = pd.DataFrame.sort(df3, columns='calms')
df4 = df3.loc[df3['calms'] == 1]
df6 = df5[:-1]
coordinate = df6['geom']
id_row = df6[df6['geom'] == coordinate]


######------Point analysis for the location with the longest calm------########

geolocator = Nominatim()
loc = coordinate.iloc[0].centroid
#location = geolocator.reverse("50.35962183274544, 12.96941145516576 ")
#print(location.address)

fig, ax = plt.subplots()
my_weather = coastdat.get_weather(
    conn, coordinate.iloc[0].centroid, year)  # center of the square
figure = plt.figure()
my_weather.data.v_wind.plot()
plt.title('Wind speed longest calm location'.format(year))
ax.set_ylabel('wind speed in m/s')
figure.savefig(os.path.join('Plots/wind_speed_longest_calm_location', 'Wind_' +
                            'speed_longest_calm_location_{0}'.format(year)))
figure.set_tight_layout(True)
plt.close()

#f = coordinate.iloc[0].centroid
#f1 = int(f)
#print(f1)
#print('Longest Calm located: ', coordinate.iloc[0].centroid)
#geo = geopy.Polygon(coordinate)

# Reshape data into matrix
matrix_wind = []
wind_feedin = E126_power_plant.feedin(weather=my_weather, installed_capacity=1)

# Number of days of the year
if year in [2000, 2004, 2008, 2012]:
    days = 366  # leap year
else:
    days = 365
matrix_wind = np.reshape(wind_feedin, (days, 24))
a = np.transpose(matrix_wind)
b = np.flipud(a)
fig, ax = plt.subplots()

# Plot image
figure = plt.figure()
plt.imshow(b, cmap='afmhot', interpolation='nearest',
           origin='lower', aspect='auto', vmax=power_limit)

plt.title('Wind feedin {0} nominal power < {1}'.format(year, power_limit))
ax.set_xlabel('days of year')
ax.set_ylabel('hours of day')
clb = plt.colorbar()
clb.set_label('P_Wind')
figure.savefig(os.path.join('Plots/wind_feedin', 'Wind_feedin_' + str(year) +
                            '_nominal_power_lower_' + str(power_limit) +
                            '.pdf'))
figure.set_tight_layout(True)
plt.close()

#######--------------Plot the result in a map-------------------------#########

figure = plt.figure()
example = geoplot.GeoPlotter(df3['geom'], (3, 16, 47, 56),  # region of germany
                             data=df3['calms'])
example.cmapname = 'inferno'

#example.geometries = germany['geom'] -> Netzregionen

example.plot(edgecolor='black', linewidth=1, alpha=1)

print('creating plot...')
plt.title('Longest calms Germany {0}'.format(year))
# create legend by longest calm
example.draw_legend(legendlabel="Length of wind calms < 5 % P_nenn in h",
                    extend='neither', tick_list=[0, np.amax(calm_list) * 0.25,
                                                 np.amax(calm_list) * 0.5,
                                                 np.amax(calm_list) * 0.75,
                                                 np.amax(calm_list)])
example.basemap.drawcountries(color='white', linewidth=2)
example.basemap.shadedrelief()
example.basemap.drawcoastlines()
plt.box(on=None)
figure.savefig(os.path.join('Plots/longest_calms_germany',
                            'longest_calms_germany_{0}'.format(year)))
figure.set_tight_layout(True)
plt.close()
