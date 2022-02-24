from matplotlib import rcParams
rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Helvetica']
import pandas as pd
import numpy as np
import os
import datetime
import seaborn as sns
import matplotlib.pyplot as plt

# Class module for script
class data_assessment():
    def __init__(self):
        self.data_filepath = '/Volumes/Hamish_ext/OVO2/'
        self.aggregate_filepath = '/Volumes/Hamish_ext/Project_Jua/Aggregated_data/Separated_files/'
        self.project_filepath = '~/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/'
        self.counties = ['Turkana', 'Kilifi', 'Isiolo', 'Kwale', 'Taita Taveta']
        self.data_sheet = pd.read_csv(self.project_filepath + 'OVO2_site_info.csv')
        self.local_data_filepath = self.project_filepath + 'Data/'

    # Function used to select the best sites
    def site_identification(self):

        # Import both site information files and highlight relevant columns
        input_file = pd.read_csv(self.aggregate_filepath + 'Site_Information_test_final.csv')
        input_file_2 = pd.read_csv(self.project_filepath + 'OVO2_site_info_update.csv')
        input_file_2 = input_file_2.set_index('Site Name')
        days_data = input_file['Days of Data']
        days_between = input_file['Days between Start and End']
        site = input_file['Site Name']
        state = input_file['State']
        days_between = input_file['Days between Start and End']
        days_data = input_file['Days of Data']
        #data_completeness = input_file['Data Completeness']

        # Create empty lists to append to
        both_files_days_data = []
        both_files_days_between = []
        error_files = []

        # Loop through filenames in second dataset
        for row in range(0, len(input_file)):

            current_site = site[row]
            current_state = state[row]
            current_days_data = days_data.iloc[row]
            current_days_between = days_between.iloc[row]
            file_name_search = current_site + ' - ' + current_state

            # Attempt to find values in second dataset and append totals to lists
            try:
                search_values = input_file_2.filter(like=file_name_search, axis=0)
                search_days_data = search_values['Days of Data'].values
                search_days_between = search_values['Days between Start and End'].values
                both_files_days_data.append(float(search_days_data + current_days_data))
                both_files_days_between.append(float(search_days_between + current_days_between))

            except:

                error_files.append(file_name_search)
                both_files_days_data.append(0)
                both_files_days_between.append(0)

        both_files_days_data = pd.DataFrame(both_files_days_data)
        both_files_days_between = pd.DataFrame(both_files_days_between)
        both_files_data_completeness = both_files_days_data / both_files_days_between
        both_files_days_data.columns = ['Days of Data ALL']
        both_files_days_between.columns = ['Days between Start and End ALL']
        both_files_data_completeness.columns = ['Data Completeness ALL']


        all_sheet = pd.concat([input_file, both_files_days_data, both_files_days_between, both_files_data_completeness], axis=1)

        ranked_completeness = all_sheet.sort_values(['Data Completeness ALL'], axis=0, ascending=True)
        ranked_quantity = all_sheet.sort_values(['Days of Data ALL'], axis=0, ascending=True)

        # for p in range(0, len(all_sheet)):
        #     target = input_file['Site Name'].iloc[p]
        #     print(target.index.values)

        percentile = 10
        top_number = int((len(all_sheet) / 100) * percentile)

        in_both = []
        top_completeness = all_sheet.nlargest(top_number, ['Data Completeness ALL'])
        top_quantity = all_sheet.nlargest(top_number, ['Days of Data ALL'])
        top_completeness_list = top_completeness['Site Name']
        top_quantity_list = top_quantity['Site Name']

        for i in range(0, len(top_completeness)):
            target = top_completeness_list.iloc[i]
            for j in range(0, len(top_completeness)):
                target_2 = top_quantity_list.iloc[j]
                if target == target_2:
                    in_both.append(target_2)

        print(len(in_both))
        in_both_true = []
        for site in range(0, len(all_sheet)):
            current_site = all_sheet['Site Name'].iloc[site]
            if current_site in in_both:
                in_both_true.append('True')
                print('True')
            else:
                in_both_true.append('False')

        in_both_true = pd.DataFrame(in_both_true)
        in_both_true.columns = ['Top 10% for Completeness and Quantity ALL']
        all_sheet = pd.concat([all_sheet, in_both_true], axis=1)
        all_sheet.to_csv(self.project_filepath + 'Data/2022/ALL_data_sheet_10.csv')


    def data_check(self):

        # Set directory to examine files in
        directory = os.fsencode(self.aggregate_filepath)

        # Set empty lists to append site information to
        list_sites = []
        state_site = []
        start_site = []
        end_site = []
        len_site = []
        days_site = []
        difference_days_site = []
        missing_days_site = []

        days = ''
        start = ''
        end = ''
        total_days_difference = ''

        # Step through each file in the directory
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            print(filename)

            # Get rid of temp files
            if filename.startswith('.'):
                pass

            else:
                # Isolate CSV files to work on
                if filename.endswith(".csv"):  # Make sure CSV

                    # Open file
                    open_file = pd.read_csv(self.aggregate_filepath + filename)
                    list_sites.append(filename[:-4])  # Remove .csv and append the site name to list
                    len_site.append(len(open_file))  # Open the file to work on
                    # state = ''
                    state = open_file['County'][0]
                    state_site.append(state)

                    # Extract relevant date and time items from open file
                    date_col = open_file['PeriodDate']
                    hour_col = open_file['HourBucket']
                    first_date, last_date = date_col.iloc[0], date_col.iloc[-1]
                    first_hour, last_hour = hour_col.iloc[0], hour_col.iloc[-1]
                    first_hour, last_hour = f"{first_hour:02d}",  f"{last_hour:02d}"
                    first_date_hour = first_date + ' ' + str(first_hour) + ':00'
                    last_date_hour = last_date + ' ' + str(last_hour) + ':00'

                    # Append start and end to lists
                    start_site.append(first_date_hour)
                    end_site.append(last_date_hour)


                    # for county in self.counties:
                    #     if county in filename:
                    #         state = county
                    #         break
                    #     else:
                    #         pass
                    # state_site.append(state)
                    # try:
                    #     time = open_file['time_stamp']
                    #     start = time.iloc[0]
                    #     start_site.append(start)
                    #     end = time.iloc[-1]
                    #     end_site.append(end)
                    #     minus_dupes = time.str.slice(start=0, stop=-3)
                    #     minus_dupes = minus_dupes.drop_duplicates(keep='first')
                    #     days = (len(minus_dupes))/1440
                    #     days_site.append(days)
                    #
                    # except:
                    #     try:
                    #         time = open_file['Timestamp']
                    #         start = time.iloc[0]
                    #         start_site.append(start)
                    #         end = time.iloc[-1]
                    #         end_site.append(end)
                    #         minus_dupes = time.str.slice(start=0, stop=-3)
                    #         minus_dupes = minus_dupes.drop_duplicates(keep='first')
                    #         minus_dupes.to_csv('/Volumes/Hamish_ext/time_outputs/' + filename)
                    #         days = (len(minus_dupes)) / 1440
                    #         days_site.append(days)
                    #
                    #     except:
                    #         print('error file, no timestamp column')
                    #         start_site.append('')
                    #         end_site.append('')
                    #         days_site.append('')
                    # print('list site:', filename)
                    # print('Days:', days)
                    # print('State:', state)
                    # print('Start:', start)
                    # print('end', end)
                    # print('len site:', len_site[-1])

                    # Find the number of days covered by the data (not accounting for gaps)
                    dt_start = datetime.datetime.strptime(first_date_hour, '%d/%m/%Y %H:%M')
                    dt_end = datetime.datetime.strptime(last_date_hour, '%d/%m/%Y %H:%M')  # '%Y-%m-%d %H:%M:%S'
                    difference = dt_end - dt_start
                    difference_seconds = difference.seconds
                    difference_days = difference.days
                    total_days_difference = difference_days + (difference_seconds / 86400)
                    difference_days_site.append(total_days_difference)

                    # Find the amount of data in days and missing data in days
                    days = len(open_file) / 24
                    days_site.append(days)
                    missing_days_site.append(total_days_difference - days)


                    # try:
                    #
                    #     # difference = dt_end - dt_start
                    #     # difference_seconds = difference.seconds
                    #     # difference_days = difference.days
                    #     # total_days_difference = difference_days + (difference_seconds / 86400)
                    #     # difference_days_site.append(total_days_difference)
                    #     # missing_days = total_days_difference - days
                    #     # missing_days_site.append(missing_days)
                    #
                    # except:
                    #     pass
                    #     # print('error in calculating time info ')
                    #     # difference_days_site.append('')
                    #     # missing_days_site.append('')
                    # break
                    # # print('difference days:', total_days_difference)
                    # # print('missing days:',missing_days_site[-1])


        list_sites = pd.DataFrame(list_sites)
        list_sites.columns = ['Site Name']

        state_site = pd.DataFrame(state_site)
        state_site.columns = ['State']

        start_site = pd.DataFrame(start_site)
        start_site.columns = ['Start Date']

        end_site = pd.DataFrame(end_site)
        end_site.columns = ['End Date']

        len_site = pd.DataFrame(len_site)
        len_site.columns = ['Number of Measurements']

        days_site = pd.DataFrame(days_site)
        days_site.columns = ['Days of Data']

        difference_days_site = pd.DataFrame(difference_days_site)
        difference_days_site.columns = ['Days between Start and End']

        missing_days_site = pd.DataFrame(missing_days_site)
        missing_days_site.columns = ['Days missing']

        site_info = pd.concat([list_sites, state_site, len_site, start_site, end_site, days_site, difference_days_site,
                               missing_days_site], axis=1)
        site_info.to_csv(self.aggregate_filepath + 'Site_Information_test_final.csv')


    def simple_utilisation(self):

        site_list = pd.read_csv(self.local_data_filepath + 'top_5_sites.csv')['Site Name']
        #print(site_list)
        for i in range(0, len(site_list)):
            print(site_list[i])
            input_file = pd.read_csv(self.local_data_filepath + site_list[i] + '.csv')
            timestamp = input_file['Timestamp']

            for j in range(0,len(input_file)):
                print(timestamp[j])



            # supply_current = input_file['Supply Current']
            # supply_voltage = input_file['Supply Voltage']
            # #supply_power = supply_current * supply_voltage
            #
            # panel_current = input_file['Panel Current']
            # panel_voltage = input_file['Panel Voltage']
            #
            #timestamp = input_file['Timestamp']
            #
            # #room_temperature = input_file['Room Temperature']
            # #battery_voltage = input_file['Battery Voltage']
            #
            # panel_power_output = pd.DataFrame(panel_current * panel_voltage)
            # panel_power_output.columns = ['Panel Power Output']
            #
            # supply_power = pd.DataFrame(supply_current * supply_voltage)
            # supply_power.columns = ['Supply Power']
            #
            # output_file_power = pd.concat([timestamp + supply_power + panel_power_output], axis=1)
            # output_file_power.to_csv(self.local_data_filepath + site_list[i] + '_update.csv')

            break



    def make_radiation_averages(self):

        input_file = pd.read_csv('/Users/Hamish/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/Data/Silaloni Primary School - solar.csv')

        average_solar = np.array([])
        std_solar = np.array([])

        str_start_time = '00:00:00'
        start_time = datetime.datetime.strptime(str_start_time, '%H:%M:%S')
        one_hour = datetime.timedelta(hours=1)

        count = 0
        for hour in range(0, 24):
            searchtime = start_time + (one_hour * count)
            searchtime_str = str(datetime.datetime.strftime(searchtime, '%H:%M:%S'))

            solar = np.array([])
            for row in range(0, len(input_file)):
                current_row = input_file.iloc[row]
                if current_row['local_time'] == searchtime_str:
                    solar = np.append(solar, current_row['irradiance_direct'])
                else:
                    pass

            average_solar = np.append(average_solar, np.mean(solar))
            std_solar = np.append(std_solar, np.std(solar))

            count += 1

        average_solar = pd.DataFrame(average_solar)
        average_solar.columns = ['Average Solar']
        std_solar = pd.DataFrame(std_solar)
        std_solar.columns = ['Std_solar']
        output_solar = pd.concat([average_solar, std_solar],axis=1)
        output_solar.to_csv('/Users/Hamish/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/Data/solar_kwale.csv')

    def make_hourly_averages(self):

        input_file = pd.read_csv('/Users/Hamish/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/Data/Silaloni '
                                 'Primary School - Kwale_hourly_no_gaps_hours.csv')

        average_supplied = np.array([])
        std_supplied = np.array([])

        average_panel = np.array([])
        std_panel = np.array([])

        average_panel_voltage = np.array([])
        std_panel_voltage = np.array([])

        average_battery_voltage = np.array([])
        std_battery_voltage = np.array([])

        str_start_time = '00:00:00'
        start_time = datetime.datetime.strptime(str_start_time, '%H:%M:%S')
        one_hour = datetime.timedelta(hours=1)

        count = 0
        for hour in range(0,24):
            searchtime = start_time + (one_hour * count)
            searchtime_str = str(datetime.datetime.strftime(searchtime,'%H:%M:%S'))

            supplied = np.array([])
            panel = np.array([])
            p_voltage = np.array([])
            b_voltage = np.array([])

            for row in range(0, len(input_file)):
                current_row = input_file.iloc[row]
                if current_row['Timestamp'] == searchtime_str:

                    supplied = np.append(supplied, current_row['Energy Supplied'])
                    panel = np.append(panel, current_row['Panel Energy'])
                    p_voltage = np.append(p_voltage, current_row['Panel Voltage'])
                    b_voltage = np.append(b_voltage, current_row['Battery Voltage'])


                else:
                    pass

            average_supplied = np.append(average_supplied, np.mean(supplied))
            std_supplied = np.append(std_supplied, np.std(supplied))

            average_panel = np.append(average_panel, np.mean(panel))
            std_panel = np.append(std_panel, np.std(panel))

            average_panel_voltage = np.append(average_panel_voltage, np.mean(p_voltage))
            std_panel_voltage = np.append(std_panel_voltage, np.std(p_voltage))

            average_battery_voltage = np.append(average_battery_voltage, np.mean(b_voltage))
            std_battery_voltage = np.append(std_battery_voltage, np.std(b_voltage))

            count += 1

        average_supplied = pd.DataFrame(average_supplied)
        average_supplied.columns = ['Average Energy Supplied']
        std_supplied = pd.DataFrame(std_supplied)
        std_supplied.columns = ['Std Dev Energy Supplied']


        average_panel = pd.DataFrame(average_panel)
        average_panel.columns = ['Average Panel Energy Output']
        std_panel = pd.DataFrame(std_panel)
        std_panel.columns = ['Std Dev Panel Energy Output']

        average_panel_voltage = pd.DataFrame(average_panel_voltage)
        average_panel_voltage.columns = ['Average Panel Voltage']
        std_panel_voltage = pd.DataFrame(std_panel_voltage)
        std_panel_voltage.columns = ['Std Dev Panel Voltage']

        average_battery_voltage = pd.DataFrame(average_battery_voltage)
        average_battery_voltage.columns = ['Average Battery Voltage']
        std_battery_voltage = pd.DataFrame(std_battery_voltage)
        std_battery_voltage.columns = ['Std Dev Battery Voltage']

        output_file = pd.concat([average_supplied, std_supplied, average_panel, std_panel, average_panel_voltage,
                                 std_panel_voltage, average_battery_voltage, std_battery_voltage], axis=1)
        output_file.to_csv(self.local_data_filepath + 'Test_site_hours.csv')


    def plot_seaborn(self):

        input_file = pd.read_csv('/Users/Hamish/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/Data/Silaloni Primary School_hours.csv')
        x = input_file['Hour']
        energy_supplied = input_file['Average Energy Supplied']
        panel_energy = input_file['Average Panel Energy Output']
        upper_es = energy_supplied + (input_file['Std Dev Energy Supplied']/2)
        lower_es = energy_supplied - (input_file['Std Dev Energy Supplied']/2)
        upper_pa = panel_energy + (input_file['Std Dev Panel Energy Output']/2)
        lower_pa = panel_energy - (input_file['Std Dev Panel Energy Output']/2)
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.plot(x, energy_supplied, label='Energy Supplied')
        ax.plot(x, panel_energy, label='Panel Energy')
        ax.plot(x, lower_es, color='tab:blue', alpha=0.1)
        ax.plot(x, upper_es, color='tab:blue', alpha=0.1)
        ax.plot(x, lower_pa, color='tab:orange', alpha=0.1)
        ax.plot(x, upper_pa, color='tab:orange', alpha=0.1)
        ax.fill_between(x, lower_es, upper_es, alpha=0.2)
        ax.fill_between(x, lower_pa, upper_pa, alpha=0.2)
        ax.set_xlabel('Hour')
        ax.set_ylabel('Energy (kWh)')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_ylim([0,350])
        ax.set_xlim([0,23])
        leg = plt.legend(loc='upper left')
        #Axis 2
        ax2 = ax.twinx()  # instantiate a second axes that shares the same x-axis
        ax2.set_ylabel('Voltage')

        battery = input_file['Average Battery Voltage']
        #upper_battery = battery + (input_file['Std Dev Battery Voltage']/2)
        #lower_battery = battery - (input_file['Std Dev Battery Voltage']/2)
        ax2.plot(x, battery, label='Battery Voltage',color='tab:purple',linestyle='dashed')
        #ax2.plot(x, lower_battery, color='tab:purple', alpha=0.1)
        #ax2.plot(x, upper_battery, color='tab:purple', alpha=0.1)
        #ax2.fill_between(x, lower_battery, upper_battery, alpha=0.2)
        #ax2.set_ylim([0,80])
        leg = plt.legend(loc='upper right')
        plt.title('Silaloni Primary School')
        plt.show()

    def plot_seaborn_solar(self):

        input_file = pd.read_csv('/Users/Hamish/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/Data/solar_kwale.csv')
        input_file2 = pd.read_csv(
            '/Users/Hamish/Library/Mobile Documents/com~apple~CloudDocs/Project Jua/Data/Silaloni Primary School_hours.csv')
        x = input_file['Hour']
        solar = input_file['Average Solar']
        upper = solar + (input_file['Std_solar']/2)
        lower = solar - (input_file['Std_solar']/2)
        panel_energy = input_file2['Average Panel Energy Output']
        upper_pa = panel_energy + (input_file2['Std Dev Panel Energy Output']/2)
        lower_pa = panel_energy - (input_file2['Std Dev Panel Energy Output']/2)


        fig, ax = plt.subplots(figsize=(9, 5))
        ax.set_ylabel('Energy (kWh)')
        ax.plot(x, panel_energy, label='Panel Energy', color='tab:orange')
        ax.plot(x, lower_pa, color='tab:orange', alpha=0.1)
        ax.plot(x, upper_pa, color='tab:orange', alpha=0.1)
        ax.fill_between(x, lower_pa, upper_pa, alpha=0.2, color='tab:orange')
        ax.set_ylim([0,350])
        leg = plt.legend(loc='upper left')

        ax2 = ax.twinx()  # instantiate a second axes that shares the same x-axis
        ax2.plot(x, solar, label='Solar Irradiance', color='brown')
        ax2.plot(x, lower, color='brown', alpha=0.1)
        ax2.plot(x, upper, color='brown', alpha=0.1)
        ax2.fill_between(x, lower, upper, alpha=0.2, color='brown')
        ax2.set_xlabel('Hour')
        ax2.set_ylabel('Solar Irradiance (W/m$^2$)')
        ax2.set_xlim([0, 23])
        ax2.set_ylim([0, 0.7])
        leg = plt.legend(loc='upper right')
        plt.title('Kwale Solar Irradiance')

        plt.show()

#data_assessment().make_radiation_averages()
data_assessment().site_identification()