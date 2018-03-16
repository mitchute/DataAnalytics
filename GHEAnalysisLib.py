import fnmatch
import json
import os
import sys
from datetime import datetime

import pandas as pd

# Shortcut
fpath = os.path.join


def combine_date_time(date, time):
    # Worker function to combine two date time objects; one containing the date, the other containing the time.
    new_dt = datetime(date.year, date.month, date.day, time.hour, time.minute, time.second)
    return new_dt


def import_all_spain_data_sets_in_dir_to_dataframe(path):
    # Step 1: load all data from raw csv files
    try:
        dfs = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                if fnmatch.fnmatch(filename, 'reference.xlsx'):
                    pass
                elif fnmatch.fnmatch(filename, '*.xlsx'):
                    try:
                        print(filename)
                        filepath = fpath(dirpath, filename)

                        # Import raw sheets from file
                        df_sys = pd.read_excel(filepath, sheet_name='System', parse_dates=['Day', 'Time'])
                        df_ghe = pd.read_excel(filepath, sheet_name='Boreholes', parse_dates=['Day', 'Time'])

                        # Drop any rows with null values in date
                        df_sys = df_sys[df_sys.Day.notnull()]
                        df_ghe = df_ghe[df_ghe.Day.notnull()]

                        # Combine the separate date and time columns into a single datetime object
                        date_sys = pd.DataFrame(
                                    [combine_date_time(df_sys['Day'][x], df_sys['Time'][x]) for x in range(len(df_sys['Day']))],
                                    columns=['Date'])
                        date_ghe = pd.DataFrame(
                                    [combine_date_time(df_ghe['Day'][x], df_ghe['Time'][x]) for x in range(len(df_ghe['Day']))],
                                    columns=['Date'])
                        df_sys["Date"] = date_sys
                        df_ghe["Date"] = date_ghe

                        # Drop the previous date and time columns
                        df_sys.drop(['Day', 'Time'], axis=1, inplace=True)
                        df_ghe.drop(['Day', 'Time'], axis=1, inplace=True)

                        # Get rid of the masculine ordinal symbol. I think it's ASCII, but just in case...
                        df_sys.columns = df_sys.columns.str.replace('º', '')
                        df_ghe.columns = df_ghe.columns.str.replace('º', '')

                        # Merge the different df's corresponding to each sheet from this file
                        df_merge = pd.merge(df_sys, df_ghe, on="Date")

                        # Save for later
                        dfs.append(df_merge)
                    except:
                        print('File failed to import')

        # Merge final df
        df_final = pd.concat(dfs)

        # Make Date the index
        df_final.set_index("Date", inplace=True)

        # Sort dates in chronological order
        df_final.sort_index(inplace=True)

        # Set data types as floats
        df_final = df_final.astype('float64')

        return df_final
    except:
        print('Data failed to import')


def load_stored_dataframes_from_csv_to_single_dataframe(path):
    # Step 2: load the yearly csv files into a single data frame.
    try:
        dfs = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                if fnmatch.fnmatch(filename, '*_Raw.csv'):
                    try:
                        # Load the csv files
                        print('Reading {}'.format(filename))
                        df = pd.read_csv(fpath(dirpath, filename), index_col='Date', parse_dates=['Date'])
                        dfs.append(df)
                    except:
                        print('Failed to import {}'.format(filename))

        # Merge data from the individual dataframes into a single dataframe
        df_final = pd.concat(dfs)

        # Sort the final dataframe dates
        df_final.sort_index(inplace=True)

        # Sort the final dataframe columns alphabetically
        df_final.sort_index(axis=1, inplace=True)

        # Drops any duplicates by date/time. Keeps the first, deletes the rest
        df_final.drop_duplicates()

        print('Dataframes loaded successfully')

        return df_final
    except:
        print('Failed to load data frames')


def get_spain_resource_files(path):

    dfs = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        for filename in filenames:
            if fnmatch.fnmatch(filename, 'reference.xlsx'):
                filepath = fpath(dirpath, filename)
                df = pd.read_excel(filepath, sheet_name='Operational Days', parse_dates=[0], usecols=3)
                df.columns = ['Sys Working', 'Representative Data', 'STD/OPT']
                dfs.append(df)

    # Merge data from the individual dataframes into a single dataframe
    df_final = pd.concat(dfs)

    # Sort the final dataframe dates
    df_final.sort_index(inplace=True)

    return df_final


def write_annual_csv_files(df, start_year, end_year, path, basename):
    # Step 4: Write the yearly, raw, deduped data to annual csv files.

    for year in range(start_year, end_year +1):
        print('Writing year: {}'.format(year))
        start_date = '{}-01-01'.format(year)
        end_date = '{}-12-31'.format(year)
        file_name = '{}_{}.csv'.format(year, basename)
        df_annual = df.ix[start_date:end_date]
        df_annual.to_csv(fpath(path, file_name))


def import_excel_files_to_single_dataframe(path, dict_name):

    # Read and load the json file with info about each file
    with open(fpath(path, dict_name)) as f:
        file_info_dict = json.loads(f.read())

    # Temporary list to store dataframe while we read in information
    dfs = []

    # Walk files in json file
    for f_name in file_info_dict:
        print('Importing File: {}'.format(f_name))

        # Shortcut
        worksheets = file_info_dict[f_name]

        # Walk worksheets in this file
        for sheet_name in worksheets:

            print('\tSheet: {}'.format(sheet_name))

            # Shortcut
            this_sheet = file_info_dict[f_name][sheet_name]

            # pd.read_excel arguments
            header = this_sheet['header']
            index_col = this_sheet['index_col']
            usecols = this_sheet['usecols']
            other_names = this_sheet['other_names']

            # Read the excel file
            df = pd.read_excel(fpath(path, f_name),
                               sheet_name=sheet_name,
                               header=header,
                               index_col=index_col,
                               parse_dates=[index_col],
                               names=other_names,
                               usecols=usecols)
            df.sort_index(inplace=True)

            # Store the temporary dataframes so they can be merged later
            dfs.append(df)

    # Merge all of the dataframe for each sheet from each file
    df_final = pd.concat(dfs)

    # Sort the final dataframe dates
    df_final.sort_index(inplace=True)

    # Sort the final dataframe columns alphabetically
    df_final.sort_index(axis=1, inplace=True)

    # Drops any duplicates by date/time. Keeps the first, deletes the rest
    df_final.drop_duplicates()

    # Resample the data on an hourly interval
    df_final = df_final.resample('60T').mean()

    return df_final


def fill_dataframe_null_vals(df, path, dict_name):

    # Read and load the json file with info about each file
    with open(fpath(path, dict_name)) as f:
        fill_info_dict = json.loads(f.read())

    for col_name in fill_info_dict:
        print('Filling Column: {}'.format(col_name))

        method = fill_info_dict[col_name].lower()


        ts = df[col_name]

        # Refer to: https://pandas.pydata.org/pandas-docs/stable/missing_data.html

        if method == 'interp-time':
            ts = ts.interpolate(method='time')
        elif method == 'interp':
            ts = ts.interpolate()
        elif method == 'ffill':
            ts = ts.fillna(method='ffill')
        elif method == 'bfill':
            ts = ts.fillna(method='bfill')
        elif method == 'mean-column':
            ts = ts.fillna(ts.mean())
        elif method == 'mean':
            ts = ts.fillna(fill_with_mean_from_surrounding_vals(ts))
        elif method == 'fsmear':
            ts = ts.fillna(fill_with_forward_smear(ts))
        elif method == 'bsmear':
            ts = ts.fillna(fill_with_backward_smear(ts))

        df[col_name] = ts

    return df


def fill_with_mean_from_surrounding_vals(series):

    null_indices = []
    data_indices = []

    for i, curr_val in enumerate(series):
        if pd.isnull(curr_val):
            null_indices.append(i)
        else:
            data_indices.append(i)

    for i, data_index in enumerate(data_indices):
        if i == 0:
            pass
        else:
            u_index = data_index
            l_index = data_indices[i-1]
            if (u_index - l_index) > 1:
                u_val = series[u_index]
                l_val = series[l_index]
                fill_val = (u_val + l_val) / 2.0
                num_null_indices_to_pop = 0
                for j, null_index in enumerate(null_indices):
                    if null_index < l_index:
                        num_null_indices_to_pop += 1
                        pass
                    elif u_index < null_index:
                        for k in range(num_null_indices_to_pop):
                            null_indices.pop(0)
                        break
                    else:
                        series[null_index] = fill_val

    return series


def fill_with_forward_smear(series):

    null_indices = []
    data_indices = []

    # Get the data and null indices first
    for i, curr_val in enumerate(series):
        if pd.isnull(curr_val):
            null_indices.append(i)
        else:
            data_indices.append(i)

    # Forward-fill all but missing values between valid data
    for i, data_index in enumerate(data_indices):
        if i == 0:
            pass
        else:
            u_index = data_index
            l_index = data_indices[i-1]
            if (u_index - l_index) > 1:

                fill_val = series[l_index] / (u_index - l_index)

                # Fill the original data point with the forward-smear val
                series[l_index] = fill_val

                num_null_indices_to_pop = 0

                for j, null_index in enumerate(null_indices):
                    if null_index < l_index:
                        num_null_indices_to_pop += 1
                        pass
                    elif u_index < null_index:
                        # Remove null indices from list as we go
                        for k in range(num_null_indices_to_pop):
                            null_indices.pop(0)
                        break
                    else:
                        series[null_index] = fill_val

    # Fill missing values at end of list
    num_null_indices_to_pop = 0

    # Figure out how many need to be removed first
    for i, null_index in enumerate(null_indices):
        if null_index < data_indices[-1]:
            num_null_indices_to_pop += 1

    # Remove any that are already done
    for i in range(num_null_indices_to_pop):
        null_indices.pop(0)

    # Forward fill data at the end of the list
    fill_val = series[data_indices[-1]] / (len(null_indices) + 1)
    series[data_indices[-1]] = fill_val

    for i, null_index in enumerate(null_indices):
        series[null_index] = fill_val

    return series


def fill_with_backward_smear(series):

    null_indices = []

    # Backward fill the null vals
    for i, curr_val in enumerate(series):
        if pd.isnull(curr_val):
            null_indices.append(i)
        else:
            if null_indices:
                fill_val = series[i] / (len(null_indices) + 1)

                for null_index in null_indices:
                    series[null_index] = fill_val

                series[i] = fill_val
                null_indices = []

    return series


if __name__ == '__main__':
    # df = import_all_spain_data_sets_in_dir_to_dataframe('.')
    # df = load_stored_dataframes_from_csv_to_single_dataframe(sys.argv[1])
    # df = write_annual_csv_files(sys.argv[1])
    df = import_excel_files_to_single_dataframe(sys.argv[1], sys.argv[2])
    df = fill_dataframe_null_vals(df, sys.argv[1], sys.argv[3])
    pass
