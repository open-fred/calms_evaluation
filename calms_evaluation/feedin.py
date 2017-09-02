import copy
import pvlib
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain


def pv(multi_weather, module_name, inverter_name, azimuth, tilt, albedo):
    print('Calculating PV feedin...')
    pv_feedin = {}

    smodule = {
        'module_parameters': pvlib.pvsystem.retrieve_sam('sandiamod')[
            module_name],
        'inverter_parameters': pvlib.pvsystem.retrieve_sam('sandiainverter')[
            inverter_name],
        'surface_azimuth': azimuth,
        'surface_tilt': tilt,
        'albedo': albedo}

    number_of_weather_points = len(multi_weather)
    for i in range(len(multi_weather)):
        if i % 50 == 0:
            print('  ...weather object {0} from {1}'.format(
                str(i), str(number_of_weather_points)))
        location = {'latitude': multi_weather[i].latitude,
                    'longitude': multi_weather[i].longitude}

        weather = copy.deepcopy(multi_weather[i].data)
        weather['ghi'] = weather['dhi'] + weather['dirhi']
        weather['temp_air'] = weather.temp_air - 273.15
        weather.rename(columns={'v_wind': 'wind_speed'},
                       inplace=True)

        p_peak = (
            smodule['module_parameters'].Impo *
            smodule['module_parameters'].Vmpo)

        # pvlib's ModelChain
        mc = ModelChain(PVSystem(**smodule), Location(**location))
        mc.complete_irradiance(times=weather.index, weather=weather)
        mc.run_model(times=weather.index, weather=weather)

        feedin_scaled = mc.dc.p_mp.fillna(0) / p_peak
        feedin_scaled.name = 'feedin'
        pv_feedin[multi_weather[i].name] = feedin_scaled

    return pv_feedin