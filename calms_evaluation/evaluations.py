import pandas as pd
import numpy as np
import copy


def calculate_avg_wind_speed(multi_weather):
    avg_wind_speed = {}
    for i in range(len(multi_weather)):
        avg_wind_speed[multi_weather[i].name] = np.mean(
            multi_weather[i].data.v_wind)
    avg_wind_speed = pd.DataFrame(data=avg_wind_speed,
                                  index=['results']).transpose()
    return avg_wind_speed


def create_calms_dict(power_limit, wind_feedin):
    """
    Creates a Dictonary containing DataFrames for all locations (keys: gids of
    locations) with the wind feedin time series (column 'feedin_wind_pp') and
    information about calms (column 'calm' - calm: value of wind feedin,
    no calm: 'no_calm').
    """
    calms_dict = {}
    for key in wind_feedin:
        feedin = pd.DataFrame(data=wind_feedin[key])
        # Find calms
        calms = feedin.where(feedin < power_limit, other='no_calm')
        calms.columns = ['calm']
        calms_dict[key] = pd.concat([feedin, calms],
                                    axis=1)  # brings columns to the same level
    return calms_dict


def calculate_calms(calms_dict):
    """
    Returns the calm lengths of all the calms at each location and finds the
    longest and shortest calm from all the calms at each location.

    Returns
    -------
    calms_max : DataFrame
        indices: gids of location, data: longest calm of location.
    calms_min : DataFrame
        indices: gids of location, data: shortest calm of location.
    calm_lengths : Dictionary
        keys: gids of weather location, data: array
        Length of the single calms for each location.
    """
    calms_max, calms_min, calm_lengths = {}, {}, {}
    for key in calms_dict:
        df = calms_dict[key]
        # Find calm periods
        calms, = np.where(df['calm'] != 'no_calm')
        calm_arrays = np.split(calms, np.where(np.diff(calms) != 1)[0] + 1)
        # Write the calm lengths into array of dictionary calm_lengths
        calm_lengths[key] = np.array([len(calm_arrays[i])
                                      for i in range(len(calm_arrays))])
        # Find the longest and shortest calm from all periods
        maximum = max(calm_lengths[key])
        calms_max[key] = maximum
        minimum = min(calm_lengths[key])
        calms_min[key] = minimum
    # Create DataFrame
    calms_max = pd.DataFrame(data=calms_max, index=['results']).transpose()
    calms_min = pd.DataFrame(data=calms_min, index=['results']).transpose()
    return calms_max, calms_min, calm_lengths


def calms_frequency(calm_lengths, min_length):
    """
    Finds the frequency of calms with length >= min_length for each
    location.
    """
    calms_freq = {}
    for key in calm_lengths:
        calms_freq[key] = np.compress((calm_lengths[key] >= min_length),
                                      calm_lengths[key]).size
    calms_freq = pd.DataFrame(data=calms_freq, index=['results']).transpose()
    return calms_freq


def filter_peaks(calms_dict, power_limit):
    """
    Filteres the peaks from the calms using a running average.
    """
    # TODO: Could be run a second time with the camls_dict_filtered to filter possilble peaks again
    calms_dict_filtered = copy.deepcopy(calms_dict)
    for key in calms_dict_filtered:
        df = calms_dict_filtered[key]
        # Find calm periods
        calms, = np.where(df['calm'] != 'no_calm')
        calm_arrays = np.split(calms, np.where(np.diff(calms) != 1)[0] + 1)
        # Filter out peaks
        feedin_arr = np.array(df['feedin'])
        calm_arr = np.array(df['calm'])
        i = 0
        while i <= (len(calm_arrays) - 1):
            j = i + 1
            if j > (len(calm_arrays) - 1):
                break
            while (sum(feedin_arr[calm_arrays[i][0]:calm_arrays[j][-1] + 1]) /
                   len(feedin_arr[calm_arrays[i][0]:calm_arrays[j][-1] + 1])
                   < power_limit):
                j = j + 1
                if j > (len(calm_arrays) - 1):
                    break
            calm_arr[calm_arrays[i][0]:calm_arrays[j-1][-1] + 1] = feedin_arr[
                calm_arrays[i][0]:calm_arrays[j-1][-1] + 1]
            i = j
        df2 = pd.DataFrame(data=calm_arr, columns=['calm2'], index=df.index)
        df_final = pd.concat([df, df2], axis=1)
        df_final = df_final.drop('calm', axis=1)
        df_final.columns = ['feedin_wind_pp', 'calm']
        calms_dict_filtered[key] = df_final
    return calms_dict_filtered
