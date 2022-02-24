import numpy as np
import datetime
import csv
import pandas as pd


class Aggregate():

    project_path = '/Volumes/Hamish_ext/Project_Jua/'
    project_path_cloud = '~/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/'
    months_list = ['Jul 2021', 'Aug 2021', 'Sep 2021', 'Oct 2021', 'Nov 2021', 'Dec 2021', 'Jan 2022']


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

    # Run through all the confirmed sites in the site list
    for site_confirmed in site_list:

            print(site_confirmed)
            # Create empty DF to add to
            site_total_df = pd.DataFrame()

            # Cycle through possible months in list
            for month in Aggregate.months_list:

                # Import month file and filter to only values from site
                month_aggregate = pd.read_csv(Aggregate.project_path + 'Aggregated_data/{month} Aggregated.csv'.format(month=month))
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

            # Export the site DF to CSV file
            site_total_df.to_csv(Aggregate.project_path + 'Aggregated_data/Separated_files/' + site_confirmed + '.csv')





if __name__ == '__main__':
    main()
