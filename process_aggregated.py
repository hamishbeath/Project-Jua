import numpy as np
import datetime
import csv
import pandas as pd
from datetime import *

class Aggregate():

    project_path = '/Volumes/Hamish_ext/Project_Jua/'
    project_path_cloud = '~/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/'
    months_list = ['Jul 2021', 'Aug 2021', 'Sep 2021', 'Oct 2021', 'Nov 2021', 'Dec 2021', 'Jan 2022', 'Feb 2022',
                   'Mar 2022', 'Apr 2022', 'May 2022', 'Jun 2022']


def main():

    site_list = pd.read_csv(Aggregate.project_path + 'Aggregated_data/Site_list.csv')['Site List'].values
    #site_list = []
    # Cycle through target months
    # for month in Aggregate.months_list:
    #
    #     # Import target month file of aggregated data for all sites in month
    #     month_aggregate = pd.read_csv(Aggregate.project_path + 'Aggregated_data/{month} Aggregated.csv'.format(month=month))
    #
    #     # Filter out duplicates to give list of sites included in month's data
    #     sites = month_aggregate['SiteName'].drop_duplicates(keep='first', inplace=False).values
    #
    #     # Make sure they are in the master site list
    #     for site in sites:
    #         if site in site_list:
    #             pass
    #         else:
    #             site_list.append(site)

    # site_list = pd.DataFrame(site_list, columns=['Site List'])
    # site_list.to_csv(Aggregate.project_path + 'Aggregated_data/Site_list.csv')

    counties = []
    sites = []
    lengths = []
    institutions = []

    # Run through all the confirmed sites in the site list
    for site_confirmed in site_list:

            print(site_confirmed)
            # Create empty DF to add to
            site_total_df = pd.DataFrame()

            # Cycle through possible months in list
            for month in Aggregate.months_list:

                # Import month file and filter to only values from site
                month_aggregate = pd.read_csv(Aggregate.project_path + 'Aggregated_data/{month} Aggregated.csv'
                                              .format(month=month))
                month_aggregate = month_aggregate.set_index('SiteName')
                only_site_values = month_aggregate.filter(like=site_confirmed, axis=0)

                # If there are not values for the selected site for that month then pass
                if len(only_site_values) == 0:
                    pass
                    print('no values for site', site_confirmed, 'in', month)
                else:
                    # Append the values to the DF for this site
                    site_total_df = pd.concat([site_total_df, only_site_values], axis=0)
                    only_site_values.to_csv(Aggregate.project_path + 'only_site_values_TEST.csv')
            try:
                county = site_total_df['County'][0]
                counties.append(county)
            except:
                counties.append('N/A')
            try:
                county = site_total_df['Institution'][0]
                counties.append(county)
            except:
                counties.append('N/A')
            sites.append(site_confirmed)
            lengths.append(len(site_total_df))
            # Export the site DF to CSV file
            site_total_df.to_csv(Aggregate.project_path + 'Aggregated_data/Separated_files/' + site_confirmed + '.csv')

    zipped = list(zip(sites, counties, lengths, institutions))
    stats = pd.DataFrame(zipped, columns=['site', 'county', 'length', 'institution'])
    #print(stats)
    stats.to_csv(Aggregate.project_path + 'Aggregated_data/site_stats.csv')




"""
Function that takes the stitched files and deals with the data gaps to give a full year of data.
- Inputs: Files for each site listed in chosen sites
- Outputs: a full year of data for each site, filling in gaps 
"""
def full_year():

    # Import list of sites to use
    sites = pd.read_csv(Aggregate.project_path + 'Aggregated_data/site_stats_top_75p.csv')['site']

    # Set empty lists to be used for the all sites export file
    all_sites_filled_consumption = []   # Consumption filled
    all_sites_measured_consumption = [] # Consumption not filled
    sites_names = []                    # Site Names
    sites_counties = []                 # Site Counties
    sites_institutions = []             # Site Institution Type

    delta = timedelta(days=1)  # Time delta of one day used when cycling through existing/missing data

    # Go through each of the sites in the list
    for site in sites:
        print('Site:', site)
        # Import site list
        site_file = pd.read_csv(Aggregate.project_path + 'Aggregated_data/Separated_files/' + site + '.csv')
        # Extract relevant site variables for the all sites export sheet and append to above lists
        county, institution = site_file['County'][0], site_file['Institution'][0]
        sites_names.append(site)
        sites_counties.append(county)
        sites_institutions.append(institution)
        consumption = site_file['Consumption']
        all_sites_measured_consumption.append(np.sum(consumption))

        # Extract the hour and date columns from the site file to be used below
        hour_bucket = site_file['HourBucket']
        period_date = site_file['PeriodDate']

        # Create a new column in the DataFrame that includes both the date and the time combined, to be used below
        date_time_list = []
        for cell in range(0, len(site_file)):
            current_hour = str(hour_bucket[cell]).zfill(2)
            current_date = str(period_date[cell])
            date_time_string = current_date + ' ' + current_hour + ':00'
            date_time_list.append(date_time_string)

        date_time_list = pd.DataFrame(date_time_list, columns=['DateTime'])
        site_file = pd.concat([site_file, date_time_list], axis=1)

        # Create empty lists to be appended to over each hour
        times = []   # List of times
        consumption_log = []    # List of consumptions
        filled = []

        site_file_datetime_index = site_file.set_index('DateTime')

        day_dt = date(2021, 7, 1)  # Set start date for data period (datetime object)

        # Loop through days in a year
        for day in range(0, 365):

            # Search for 'current' hour in dataset, if exists, append values and move to next
            current_month = day_dt.strftime('%B')
            site_file_month_index = site_file.set_index('MonthName')
            site_file_month_index = site_file_month_index.filter(like=current_month, axis=0)
            site_file_month_index = pd.DataFrame(site_file_month_index)

            # Loop through hours in the day
            for hour in range(0, 24):

                day_str = day_dt.strftime('%d/%m/%Y')
                hour_filled = str(hour).zfill(2)
                search_date_time = day_str + ' ' + hour_filled + ':00'
                select_value = site_file_datetime_index.filter(like=search_date_time, axis=0)
                times.append(search_date_time)

                if len(select_value) > 0:

                    selected_consumption = float(select_value['Consumption'].values)
                    consumption_log.append(selected_consumption)
                    filled.append(0)

                # If no value found, fill with the mean of that hour in that month, or over the year if no values
                elif len(select_value) == 0:

                    filled.append(1)
                    month_hour_values = site_file_month_index[site_file_month_index['HourBucket'] == hour]

                    # If no monthly values, then take the whole dataset average for that hour
                    if len(month_hour_values) == 0:
                        all_hour_values = site_file[site_file['HourBucket'] == hour]
                        consumption_log.append(all_hour_values['Consumption'].mean())

                    # If monthly values, append the mean of the hour values for the current month
                    elif len(month_hour_values) > 0:
                        consumption_log.append(month_hour_values['Consumption'].mean())

            # Move to next day, delta variable = one day
            day_dt += delta

        zipped = list(zip(times, consumption_log, filled))
        filled_consumption_df = pd.DataFrame(zipped, columns=['Timestamp', 'Consumption', 'Generated Data (1 = Yes)'])
        filled_consumption_df.to_csv(Aggregate.project_path + 'Aggregated_data/Filled_files/'
                                     + site + '_gaps_filled.csv')
        all_sites_filled_consumption.append(np.sum(consumption_log))

    other_zipped = list(zip(sites_names, sites_counties, sites_institutions, all_sites_filled_consumption,
                            all_sites_measured_consumption))
    site_info = pd.DataFrame(other_zipped, columns=['Site Name', 'County', 'Institution', 'Annual Consumption Filled (kWh)',
                                                    'Annual Consumption Measured (kWh)'])
    site_info.to_csv(Aggregate.project_path + 'Aggregated_data/Site_Data.csv')

full_year()
#
# if __name__ == '__main__':
#     main()
