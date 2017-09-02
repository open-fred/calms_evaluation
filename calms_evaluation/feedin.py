import copy
import logging
import pvlib
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain


def pv(multi_weather, module_name, inverter_name, azimuth=180, tilt=60,
       albedo=0.2):
    r"""
    Calculates specific PV feedin for each FeedinWeather object in
    multi_weather using the pvlib's ModelChain.

    Parameters
    ----------
    multi_weather : list
        `multi_weather` is a list of :class:`feedinlib.weather.FeedinWeather`
        objects.
    module_name : string
        Name of PV module from Sandia Module database provided along with the
        pvlib.
    inverter_name : string
        Name of inverter from CEC database provided along with the pvlib.
    azimuth : float
        Azimuth angle of the module. North=0, East=90, South=180, West=270.
        Default: 180.
    tilt : float
        Tilt angle of the module. Facing up=0, facing horizon=90. Default: 60.
    albedo : float
        The ground albedo. Default: 0.2.

    Returns
    -------
    dict
        Dictionary with keys holding the FeedinWeather object's name and values
        holding the corresponding specific PV feedin as pandas.Series.

    """
    #ToDo default values prüfen

    logging.info('Calculating PV feedin...')

    # specifications of PV module
    module = {
        'module_parameters': pvlib.pvsystem.retrieve_sam('sandiamod')[
            module_name],
        'inverter_parameters': pvlib.pvsystem.retrieve_sam('sandiainverter')[
            inverter_name],
        'surface_azimuth': azimuth,
        'surface_tilt': tilt,
        'albedo': albedo}

    pv_feedin = {}
    number_of_weather_points = len(multi_weather)
    for i in range(len(multi_weather)):

        # logging info
        if i % 50 == 0:
            logging.info('  ...weather object {0} from {1}'.format(
                str(i), str(number_of_weather_points)))

        location = {'latitude': multi_weather[i].latitude,
                    'longitude': multi_weather[i].longitude}

        # set up weather dataframe to meet the needs of the pvlib
        # must contain air temperature ('temp_air')in C, wind speed
        # ('wind_speed') in m/s, global horizontal irradiance ('ghi') in W/m^2,
        # direct normal irradiance ('dni') in W/m^2 and diffuse horizontal
        # irradiance ('dhi') in W/m^2
        weather = copy.deepcopy(multi_weather[i].data)
        weather['ghi'] = weather['dhi'] + weather['dirhi']
        weather['temp_air'] = weather.temp_air - 273.15
        weather.rename(columns={'v_wind': 'wind_speed'},
                       inplace=True)

        # pvlib's ModelChain
        mc = ModelChain(PVSystem(**module), Location(**location))
        mc.complete_irradiance(times=weather.index, weather=weather)
        mc.run_model(times=weather.index, weather=weather)

        p_peak = (
            module['module_parameters'].Impo *
            module['module_parameters'].Vmpo)
        feedin_scaled = mc.dc.p_mp.fillna(0) / p_peak
        feedin_scaled.name = 'feedin'
        pv_feedin[multi_weather[i].name] = feedin_scaled

    return pv_feedin
