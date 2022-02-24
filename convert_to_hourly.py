import numpy as np
import datetime
import csv
import pandas as pd


class Hourly:
    project_path = '/Volumes/Hamish_ext/Project_Jua/OVO2/'
    project_path_cloud = '~/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/'


def calculate_the_average_for_this_hour_and_increment_the_rows(df, current_hour, index):

    total_supplied_energy = 0
    total_panel_energy = 0
    total_panel_voltage = 0
    total_battery_voltage = 0
    number_of_data_points_within_hour = 0

    while True:
        out_of_bounds = index == len(df)
        if out_of_bounds:
            break

        current_row = df.iloc[index]
        # get date column of current row
        try:
            current_row_date_str = current_row['Timestamp']

        except:
            current_row_date_str = current_row['time_stamp']
        current_row_date = datetime.datetime.strptime(current_row_date_str, "%Y-%m-%d %H:%M:%S")
        only_hour_date = current_row_date.replace(minute=0, second=0)
        row_is_in_current_hour_bucket = only_hour_date == current_hour

        if row_is_in_current_hour_bucket:

            try:
                supply_current = current_row['Supply Current']
                supply_voltage = current_row['Supply Voltage']

                panel_current = current_row['Panel Current']
                panel_voltage = current_row['Panel Voltage']

                battery_voltage = current_row['Battery Voltage']

            except:
                supply_current = current_row['supply_current']
                supply_voltage = current_row['supply_voltage']

                panel_current = current_row['panel_current']
                panel_voltage = current_row['panel_voltage']

                battery_voltage = current_row['battery_voltage']

            total_supplied_energy += (supply_current * supply_voltage)
            total_panel_energy += (panel_current * panel_voltage)
            total_panel_voltage += panel_voltage
            total_battery_voltage += battery_voltage

            index += 1
            number_of_data_points_within_hour += 1
        else:
            break

    if number_of_data_points_within_hour == 0:
        average_supplied = 0
        average_panel = 0
        average_panel_voltage = 0
        average_battery_voltage = 0
    else:
        average_supplied = total_supplied_energy / number_of_data_points_within_hour
        average_panel = total_panel_energy / number_of_data_points_within_hour
        average_panel_voltage = total_panel_voltage / number_of_data_points_within_hour
        average_battery_voltage = total_battery_voltage / number_of_data_points_within_hour

    return index, average_supplied, average_panel, average_panel_voltage, average_battery_voltage


def read_csv(imei):
    df = pd.read_csv(Hourly.project_path + '{}.csv'.format(imei))
    return df


# time format: 01/07/2016  00:22:00 "%d/%m/%Y %H:%M"
# 01-Mar-2016 00:06  %d-%b-%Y %H:%M"

def get_first_hour(df):
    try:
        start_date_str = df.iloc[0]['Timestamp']
    except:
        start_date_str = df.iloc[0]['time_stamp']
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
    current_hour = start_date.replace(minute=0, second=0)
    return current_hour


def calculate_hourly_averages(df):
    """
    Function that understands number of datapoints within current hour.
    Takes each data point within hour, multiplies current and voltage for data point
    sums to 'total' variable. Total then divided by the the length of datapoints.
    """
    print("Reformatting Data, please wait...")
    current_hour = get_first_hour(df)
    one_hour = datetime.timedelta(hours=1)
    index = 0
    havent_looked_at_all_rows = index < len(df) - 1

    # our output format: each row contains
    # [ the_start_time_of_the_hour_bucket , the_average_for_this_hour ]

    hourly_averages = list()
    # our terminal case is that we have looked at all the rows

    while havent_looked_at_all_rows:
        index, average_supplied, average_panel, average_panel_voltage, average_battery_voltage \
            = calculate_the_average_for_this_hour_and_increment_the_rows(df, current_hour, index)
        row_data = [str(current_hour), average_supplied, average_panel, average_panel_voltage, average_battery_voltage]
        hourly_averages.append(row_data)
        havent_looked_at_all_rows = index < len(df) - 1
        current_hour += one_hour

    return hourly_averages


# def calculate_hourly_averages_using_panda(df):
#     times = pd.DatetimeIndex(df.ix[:, 0])
#     hourly_averages = df.groupby([times.year, times.month, times.day, times.hour]).mean()
#     hourly_energy_averages = hourly_averages['current'] * hourly_averages['voltage']
#     return hourly_energy_averages


# def write_csv(imei, hourly_averages):
#     text = 'datetime,energy\n'
#     for row in hourly_averages:
#         text += str(row[0]) + ',' + str(row[1]) + '\n'
#     with open(
#             '/Users/hamish/Library/Mobile Documents/com~apple~CloudDocs/bboxx/Goma_Data/Hourly_Formatted' + imei + '.csv',
#             'w') as f:
#         f.write(text)


def main():

    file_list = pd.read_csv(Hourly.project_path_cloud + 'Data/2022/next_30_sites.csv')
    site_list = file_list['Site Name']
    #failed_sites = pd.read_csv(Hourly.project_path_cloud + 'Data/2022/sites_not_completed.csv')
    failed_sites = []
    error_messages = []

    for site in site_list:
        print(site)
        imei = site
        df = read_csv(imei)
        try:
            hourly_averages = calculate_hourly_averages(df)
            hourly_averages = pd.DataFrame(hourly_averages, columns=['Timestamp', 'Energy Supplied', 'Panel Energy',
                                                                     'Panel Voltage', 'Battery Voltage'])
            hourly_averages.to_csv(
                Hourly.project_path_cloud + 'Data/2022/Processed Hourly Data/' + site + '_hourly.csv')

        except Exception as e:
            e = str(e.message)
            failed_sites.append(site)
            error_messages.append(e)

    output = pd.DataFrame([failed_sites, error_messages])
    output.to_csv(project_path_cloud + 'Data/2022/failed_sites.csv')

    # write_csv(imei, hourly_averages)


# hourly_averages = calculate_hourly_averages_using_panda(df)
# hourly_averages.to_csv("/Users/hamish/Desktop/Uttah_Pradesh_Data/formatted_{imei}.csv".format(imei=imei))

if __name__ == '__main__':
    main()
