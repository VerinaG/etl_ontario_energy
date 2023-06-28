import pandas as pd


def check_output_columns(output_file_name):
    # Import csv and discard first three rows
    df = pd.read_csv(f'./data/output/{output_file_name}', skiprows=3, index_col=False)

    # Check if all columns expected are present
    all_cols = ['Delivery Date', 'Generator', 'Fuel Type', 'Measurement', 
                'Hour 1', 'Hour 2', 'Hour 3', 'Hour 4',
                'Hour 5', 'Hour 6', 'Hour 7', 'Hour 8',
                'Hour 9', 'Hour 10', 'Hour 11', 'Hour 12',
                'Hour 13', 'Hour 14', 'Hour 15', 'Hour 16',
                'Hour 17', 'Hour 18', 'Hour 19', 'Hour 20',
                'Hour 21', 'Hour 22', 'Hour 23', 'Hour 24']
    return len(df.columns) == 28 and set(df.columns).issubset(all_cols)


def check_intertie_nodes(intertie_file_names):
    import xml.etree.ElementTree as ET

    for intertie_file_name in intertie_file_names:
        intertie_tree = ET.parse(f'./data/intertie_load/{intertie_file_name}')
        intertie_root = intertie_tree.getroot()
        intertie_columns = ['Hour', 'Import', "Export"]

        for col in intertie_columns:
            if intertie_root.find(f'.//{{http://www.theIMO.com/schema}}{col}') is None: return False
    return True


def check_load_nodes(load_file_names):
    import xml.etree.ElementTree as ET

    for load_file_name in load_file_names:
        load_tree = ET.parse(f'./data/intertie_load/{load_file_name}')
        load_root = load_tree.getroot()
        load_columns = ['Total Energy', 'Total Loss', 'Total Load']

        for col in load_columns:
            if load_root.find(f'.//{{http://www.ieso.ca/schema}}MQ[{{http://www.ieso.ca/schema}}MarketQuantity="{col}"]') is None: return False
    return True


def transform_output_data(output_file_name):
    import numpy as np

    # Import csv and discard first three rows
    df = pd.read_csv(f'./data/output/{output_file_name}', skiprows=3, index_col=False)

    # Melt table to change hour columns to one datetime column
    melted_df = pd.melt(df,
                        id_vars=['Delivery Date', 'Generator', 'Fuel Type', 'Measurement'],
                        value_vars=['Hour 1', 'Hour 2', 'Hour 3', 'Hour 4',
                                    'Hour 5', 'Hour 6', 'Hour 7', 'Hour 8',
                                    'Hour 9', 'Hour 10', 'Hour 11', 'Hour 12',
                                    'Hour 13', 'Hour 14', 'Hour 15', 'Hour 16',
                                    'Hour 17', 'Hour 18', 'Hour 19', 'Hour 20',
                                    'Hour 21', 'Hour 22', 'Hour 23', 'Hour 24'],
                        var_name='Hour',
                        value_name='Energy')

    # Delete missing energy data and cast
    melted_df['Energy'].replace(' ', np.nan, inplace=True)
    melted_df.dropna(subset=['Energy'], inplace=True)
    melted_df['Energy'] = pd.to_numeric(melted_df['Energy'], errors='coerce')

    # Remove 'Hour' string from Hour column and update hours to 0-23 instead of 1-24
    melted_df['Hour'] = melted_df['Hour'].str.replace('Hour ', '')
    melted_df['Hour'] = melted_df['Hour'].astype(int) - 1

    # Create new datetime column
    melted_df['DateTime'] = (pd.to_datetime(melted_df['Delivery Date'], format='%Y-%m-%d') +
                             pd.to_timedelta(melted_df['Hour'], unit='h'))
    melted_df.drop(columns=['Delivery Date', 'Hour'], inplace=True)

    # Pivot data to create new columns for measurement types 
    transformed_df = melted_df.pivot(index=['DateTime', 'Generator', 'Fuel Type'],
                                     columns='Measurement',
                                     values='Energy')
    transformed_df.reset_index(inplace=True)

    # Rename columns
    transformed_df.rename(columns={'Fuel Type': 'FuelType',
                                   'Available Capacity': 'AvailableCapacity'},
                          inplace=True)

    # Clean new measurement columns and delete any rows without output data
    measurement_cols = ['AvailableCapacity', 'Capability', 'Forecast', 'Output']
    transformed_df[measurement_cols].replace(' ', np.nan, inplace=True)
    transformed_df.dropna(subset=['Output'], inplace=True)

    # Scale remaining energy data
    transformed_df.loc[transformed_df['AvailableCapacity'].notnull(),
                       'ScaledAvailableCapacity'] = 1
    transformed_df.loc[transformed_df['Capability'].notnull(),
                       'ScaledCapability'] = 1
    transformed_df['ScaledForecast'] = transformed_df['Forecast'] / transformed_df['AvailableCapacity']
    transformed_df.loc[transformed_df['Capability'].notnull(),
                       'ScaledOutput'] = (transformed_df['Output'] / transformed_df['Capability'])
    transformed_df.loc[transformed_df['AvailableCapacity'].notnull(),
                       'ScaledOutput'] = (transformed_df['Output'] / transformed_df['AvailableCapacity'])
    # Clean up scaled columns
    transformed_df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Write to new csv file
    transformed_df.to_csv(f'./data/output/transformed_{output_file_name}',
                          index=False)


def parse_intertie_xml(intertie_file_name):
    import xml.etree.ElementTree as ET

    intertie_tree = ET.parse(f'./data/intertie_load/{intertie_file_name}')
    intertie_root = intertie_tree.getroot()
    intertie_columns = ['Hour', 'Import', "Export"]
    intertie_rows = []

    # Find import export nodes and write to df
    for x in intertie_root.findall('.//{http://www.theIMO.com/schema}Totals/{http://www.theIMO.com/schema}Schedules/*'):
        row = []
        for y in x.iter():
            tag = y.tag.split('}')[1]
            if tag in intertie_columns: row.append(y.text)
        intertie_rows.append(row)
    return pd.DataFrame(intertie_rows, columns=intertie_columns)


def parse_load_xml(load_file_name):
    import xml.etree.ElementTree as ET

    load_tree = ET.parse(f'./data/intertie_load/{load_file_name}')
    load_root = load_tree.getroot()
    load_columns = ['Hour', 'Total Energy', 'Total Loss', 'Total Load']
    load_rows = []

    # Find load nodes and write to df
    for x in load_root.findall('.//{http://www.ieso.ca/schema}Energies/{http://www.ieso.ca/schema}HourlyConstrainedEnergy/{http://www.ieso.ca/schema}DeliveryHour'):
        row = []
        row.append(x.text)
        for y in load_columns[1:]:
            row.append(load_root.find(f'.//{{http://www.ieso.ca/schema}}Energies/{{http://www.ieso.ca/schema}}HourlyConstrainedEnergy[{{http://www.ieso.ca/schema}}DeliveryHour="{x.text}"]/{{http://www.ieso.ca/schema}}MQ[{{http://www.ieso.ca/schema}}MarketQuantity="{y}"]/{{http://www.ieso.ca/schema}}EnergyMW').text)
        load_rows.append(row)
    return pd.DataFrame(load_rows, columns=load_columns)


def transform_import_export_load_data(intertie_file_names, load_file_names, year, month):
    import re
    from datetime import datetime

    transformed_df = pd.DataFrame(columns=['Import',
                                           'Export',
                                           'TotalEnergy',
                                           'TotalLoss',
                                           'TotalLoad',
                                           'DateTime'])

    for intertie_file_name, load_file_name in zip(intertie_file_names, load_file_names):
        # Parse import/export and load xml files into one dataframe
        intertie_df = parse_intertie_xml(intertie_file_name)
        load_df = parse_load_xml(load_file_name)
        curr_df = pd.merge(intertie_df, load_df, on='Hour')
    
        # Set back hour column (0-23 instead of 0-24)
        curr_df['Hour'] = curr_df['Hour'].astype(int) - 1
        curr_df.sort_values(by=['Hour'], inplace=True)

        # Ensure data is complete for 24 hours
        num_rows = len(curr_df.index)
        if (num_rows != 24):
            for x in range(num_rows, 25):
                curr_df.loc[x] = [x]
        curr_df.fillna(0)

        # Get date to create datetime column
        date = re.search('\d{4}\d{2}\d{2}', intertie_file_name).group()
        # Create new datetime column
        curr_df['DateTime'] = (datetime.strptime(date, '%Y%m%d') +
                               pd.to_timedelta(curr_df['Hour'], unit='h'))
        curr_df.drop(columns=['Hour'], inplace=True)

        # Rename columns to match database
        curr_df.rename(columns={'Total Energy': 'TotalEnergy',
                                'Total Loss': 'TotalLoss',
                                'Total Load': 'TotalLoad'},
                       inplace=True)

        transformed_df = pd.concat([transformed_df, curr_df])
    # Write to new csv file
    transformed_file_name = f'transformed_intertie_load_{year}{month}.csv'
    transformed_df.to_csv(f'./data/intertie_load/{transformed_file_name}', index=False)