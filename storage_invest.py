# -*- coding: utf-8 -*-

"""
General description:
---------------------

The example models the following energy system:

                input/output  bgas     bel
                     |          |        |       |
                     |          |        |       |
 wind(FixedSource)   |------------------>|       |
                     |          |        |       |
 pv(FixedSource)     |------------------>|       |
                     |          |        |       |
 rgas(Commodity)     |--------->|        |       |
                     |          |        |       |
 demand(Sink)        |<------------------|       |
                     |          |        |       |
                     |          |        |       |
 pp_gas(Transformer) |<---------|        |       |
                     |------------------>|       |
                     |          |        |       |
 storage(Storage)    |<------------------|       |
                     |------------------>|       |


"""

###############################################################################
# imports
###############################################################################

# Outputlib
from oemof import outputlib

# Default logger of oemof
from oemof.tools import logger
from oemof.tools import helpers

# import oemof base classes to create energy system objects
import logging
import os
import pandas as pd
import matplotlib.pyplot as plt
import oemof.solph as solph
from oemof import db
from oemof.db import coastdat
from shapely import geometry as geopy
from feedinlib import powerplants as plants

year = 2014


conn = db.connection()
my_weather = coastdat.get_weather(
conn, geopy.Point(8.043, 52.279), year)  # Location Osnabrück

#weather data

coastDat2 = {
       'dhi': 0,
       'dirhi': 0,
       'pressure': 0,
       'temp_air': 2,
       'v_wind': 100,
       'Z0': 0}

#pv module

yingli210 = {
       'module_name': 'Yingli_YL210__2008__E__',
       'azimuth': 180,
       'tilt': 30,
       'albedo': 0.2}

#wind turbine

enerconE126 = {
       'h_hub': 135,
       'd_rotor': 127,
       'wind_conv_type': 'ENERCON E 126 7500',
       'data_height': coastDat2}

#
E126_power_plant = plants.WindPowerPlant(**enerconE126)
yingli_module = plants.Photovoltaic(**yingli210)

wind_feedin = E126_power_plant.feedin(weather=my_weather,
installed_capacity=1)
pv_feedin = yingli_module.feedin(weather=my_weather, peak_power=1)


#conn = db.connection()
#pol = c.next()
#multi_weather = coastdat.get_weather(conn, germany_u['geom'][0], year)

def optimise_storage_size(filename="storage_invest.csv", solvername='cbc',
                          debug=True, number_timesteps=8760, tee_switch=True):
    logging.info('Initialize the energy system')
    date_time_index = pd.date_range('1/1/' + str(year), periods=number_timesteps,
                                    freq='H')

    energysystem = solph.EnergySystem(timeindex=date_time_index)

    # Read data file
    full_filename = os.path.join(os.path.dirname(__file__), filename)
    data = pd.read_csv(full_filename, sep=",")
    data_demand = data['demand_el']/data['demand_el'].sum()

    ##########################################################################
    # Create oemof object
    ##########################################################################
    consumption = 5165 * 1e6
    wind_installed = 1516 * 1e3
    pv_installed = 1491 * 1e3
    grid_share = 0.75

    logging.info('Create oemof objects')
    # create gas bus
    bgas = solph.Bus(label="natural_gas")

    # create electricity bus
    bel = solph.Bus(label="electricity")

    # create excess component for the electricity bus to allow overproduction
    solph.Sink(label='excess_bel', inputs={bel: solph.Flow()})

    # Create commodity object for import electricity resource
    solph.Source(label='gridsource', outputs={bel: solph.Flow(
        nominal_value=consumption * grid_share * number_timesteps / 8760,
        summed_max=1)})

    # create fixed source object for wind
    solph.Source(label='wind', outputs={bel: solph.Flow(
        actual_value=wind_feedin, nominal_value=wind_installed, fixed=True,
        fixed_costs=20)})

    # create fixed source object for pv
    solph.Source(label='pv', outputs={bel: solph.Flow(
        actual_value=pv_feedin, nominal_value=pv_installed, fixed=True,
        fixed_costs=15)})

    # create simple sink object for demand
    solph.Sink(label='demand', inputs={bel: solph.Flow(
        actual_value=data_demand, fixed=True, nominal_value=consumption)})

    # Calculate ep_costs from capex to compare with old solph
    capex = 1000
    lifetime = 20
    wacc = 0.05
    epc = capex * (wacc * (1 + wacc) ** lifetime) / ((1 + wacc) ** lifetime - 1)

    # create storage transformer object for storage
    solph.Storage(
        label='storage',
        inputs={bel: solph.Flow(variable_costs=10e10)},
        outputs={bel: solph.Flow(variable_costs=10e10)},
        capacity_loss=0.00, initial_capacity=0,
        nominal_input_capacity_ratio=1,
        nominal_output_capacity_ratio=1,
        inflow_conversion_factor=1, outflow_conversion_factor=0.8,
        fixed_costs=35,
        investment=solph.Investment(ep_costs=epc),
    )

    ##########################################################################
    # Optimise the energy system and plot the results
    ##########################################################################

    logging.info('Optimise the energy system')

    om = solph.OperationalModel(energysystem)

    if debug:
        filename = os.path.join(
            helpers.extend_basic_path('lp_files'), 'storage_invest.lp')
        logging.info('Store lp-file in {0}.'.format(filename))
        om.write(filename, io_options={'symbolic_solver_labels': True})

    logging.info('Solve the optimization problem')
    om.solve(solver=solvername, solve_kwargs={'tee': tee_switch})

    return energysystem


def get_result_dict(energysystem):
    logging.info('Check the results')
    storage = energysystem.groups['storage']
    myresults = outputlib.DataFramePlot(energy_system=energysystem)

    demand = myresults.slice_by(obj_label='demand',
                                date_from=str(year)+'-01-01 00:00:00',
                                date_to=str(year)+'-12-31 23:00:00')

    wind = myresults.slice_by(obj_label='wind',
                              date_from=str(year)+'-01-01 00:00:00',
                              date_to=str(year)+'-12-31 23:00:00')

    pv = myresults.slice_by(obj_label='pv',
                            date_from=str(year)+'-01-01 00:00:00',
                            date_to=str(year)+'-12-31 23:00:00')

    storage_input = myresults.slice_by(obj_label='storage', type='from_bus',
                                   date_from=str(year)+'-01-01 00:00:00',
                                   date_to=str(year)+'-12-31 23:00:00')

    storage_output = myresults.slice_by(obj_label='storage', type='to_bus',
                                    date_from=str(year)+'-01-01 00:00:00',
                                    date_to=str(year)+'-12-31 23:00:00')

    storage_soc = myresults.slice_by(obj_label='storage', type='other',
                                 date_from=str(year)+'-01-01 00:00:00',
                                 date_to=str(year)+'-12-31 23:00:00')

    results_dc = {}
    results_dc['ts_storage_input'] = storage_input
    results_dc['ts_storage_output'] = storage_output
    results_dc['ts_storage_soc'] = storage_soc
    results_dc['storage_cap'] = energysystem.results[
        storage][storage].invest
    results_dc['objective'] = energysystem.results.objective

    return results_dc


def create_plots(energysystem):

    logging.info('Plot the results')

    cdict = {'wind': '#5b5bae',
             'pv': '#ffde32',
             'storage': '#42c77a',
             'gridsource': '#636f6b',
             'demand': '#ce4aff',
             'excess_bel': '#555555'}

    # Plotting the input flows of the electricity bus for January
    myplot = outputlib.DataFramePlot(energy_system=energysystem)
    myplot.slice_unstacked(bus_label="electricity", type="to_bus",
                           date_from=str(year)+'-01-01 00:00:00',
                           date_to=str(year)+'-01-31 00:00:00')
    colorlist = myplot.color_from_dict(cdict)
    myplot.plot(color=colorlist, linewidth=2, title='January'+str(year))
    myplot.ax.legend(loc='upper right')
    myplot.ax.set_ylabel('Power in MW')
    myplot.ax.set_xlabel('Date')
    myplot.set_datetime_ticks(date_format='%d-%m-%Y', tick_distance=24*7)

    # Plotting the output flows of the electricity bus for January
    myplot.slice_unstacked(bus_label="electricity", type="from_bus")
    myplot.plot(title="Year 2016", colormap='Spectral', linewidth=2)
    myplot.ax.legend(loc='upper right')
    myplot.ax.set_ylabel('Power in MW')
    myplot.ax.set_xlabel('Date')
    myplot.set_datetime_ticks()

    plt.show()

    # Plotting a combined stacked plot
    fig = plt.figure(figsize=(24, 14))
    plt.rc('legend', **{'fontsize': 19})
    plt.rcParams.update({'font.size': 19})
    plt.style.use('grayscale')

    handles, labels = myplot.io_plot(
        bus_label='electricity', cdict=cdict,
        barorder=['pv', 'wind', 'gridsource', 'storage'],
        lineorder=['demand', 'storage', 'excess_bel'],
        line_kwa={'linewidth': 4},
        ax=fig.add_subplot(1, 1, 1),
        date_from=str(year)+'-06-01 00:00:00',
        date_to=str(year)+'-06-8 00:00:00',
        )
    myplot.ax.set_ylabel('Power in MW')
    myplot.ax.set_xlabel('Date')
    myplot.ax.set_title("Electricity bus")
    myplot.set_datetime_ticks(tick_distance=24, date_format='%d-%m-%Y')
    myplot.outside_legend(handles=handles, labels=labels)

    plt.show()


def run_storage_invest_example():
    logger.define_logging()
    esys = optimise_storage_size()
    # esys.dump()
    # esys.restore()
    import pprint as pp
    results = get_result_dict(esys)
    p = results['ts_storage_soc']['val']
    print(p)
    plt.plot(p)

    plt.show()

    # Print some results
    #print(results['ts_storage_soc'])
    #print(results['storage_cap'])



    # Write results to csv
#    results['ts_storage_input'].to_csv('ts_storage_input_' + str(year) + '.csv')
#    results['ts_storage_soc'].to_csv('ts_storage_soc_' + str(year) + '.csv')

    # create_plots(esys)


if __name__ == "__main__":
    run_storage_invest_example()
