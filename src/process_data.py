import pandas as pd
import re

def merge_taxonomy_data(occurrence_gdf, taxonomy_df):
    """
    Merge taxonomy information to the occurrence data.

    Parameters:
    occurrence_gdf (geopandas.GeoDataFrame): The occurrence data GeoDataFrame.
    taxonomy_df (pandas.DataFrame): The taxonomy data DataFrame.

    Returns:
    geopandas.GeoDataFrame: The merged GeoDataFrame.
    """
    print("Joining data sets together...")
    occurrence_gdf['unit.linkings.taxon.id'] = occurrence_gdf['unit.linkings.taxon.id'].str.extract('(MX\.\d+)')
    merged_gdf = occurrence_gdf.merge(taxonomy_df, left_on='unit.linkings.taxon.id', right_on='idMainTaxon', how='left')
    return merged_gdf

def convert_dates(gdf):
    """
    Extracts and formats event dates from a GeoDataFrame.

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame containing column named eventDateTimeDisplay.

    Returns:
    tuple: A tuple containing the dates as pandas Series.
    """
    dates = gdf['eventDateTimeDisplay']

    # Define the regex patterns to find days and times
    date_regex = '[0-9]{4}-[0-9]{2}-[0-9]{2}'
    time_regex = '[0-9]{2}:[0-9]{2}'
    time2_regex = '[0-9]{1}:[0-9]{2}'

    # Loop over dates and translate into the correct form
    for i, date in enumerate(dates):
        try:
            day = re.search(date_regex, date)
            time1 = re.search(time_regex, date)
            time2 = re.search(time2_regex, date)
            if day and time1:
                datetime = day.group(0) + 'T' + time1.group(0) + 'Z'
                dates.iloc[i] = datetime
            elif day and time2:
                datetime = day.group(0) + 'T0' + time2.group(0) + 'Z'
                dates.iloc[i] = datetime
            elif day: 
                datetime = day.group(0) + 'T00:00Z'
                dates.iloc[i] = datetime
            else:
                dates.iloc[i] = None
        except TypeError:
            dates.iloc[i] = None
            print(f"{date} is not a valid date format (e.g. YYYY-MM-DD or YYYY-MM-DD [HH-MM])")


    # Convert dates to datetime format
    dates = pd.to_datetime(dates)
    return dates

def get_min_max_dates(gdf):
    """
    Finds the minimum and maximum event dates and returns them in RFC3339 format

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame containing column named eventDateTimeDisplay.

    Returns:
    first_date: the first datestamp of the GeoDataframe in RFC3399 format
    end_date: the last datestamp of the Geodataframe in RFC3399 format
    """
    # Filter out NaT (Not a Time) values
    dates = gdf['eventDateTimeDisplay']
    dates = pd.to_datetime(dates)
    dates_without_na = dates.dropna()

    # Get the minimum and maximum dates in RFC3339 format
    if len(dates_without_na) > 0:
        first_date = str(dates_without_na.min().strftime('%Y-%m-%dT%H:%M:%SZ'))
        end_date = str(dates_without_na.max().strftime('%Y-%m-%dT%H:%M:%SZ'))
        return first_date, end_date
    else:
        return None, None
    
def column_names_to_dwc(gdf, lookup_table):
    """
    Maps column names in a GeoDataFrame to Darwin Core names using a lookup table.

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame to be mapped.
    lookup_table (str): The path to the CSV lookup table.

    Returns:
    geopandas.GeoDataFrame: The GeoDataFrame with columns renamed according to the lookup table.
    """
    # Load the lookup table CSV into a DataFrame
    lookup_df = pd.read_csv(lookup_table, sep=';', header=0)

    # Map all column names according to the Darwin Core names in the lookup table
    column_mapping = {}
    for _, row in lookup_df.iterrows():
        column_mapping[row['finbif_api_var']] = row['dwc']

    # Rename columns based on the mapping
    gdf.rename(columns=column_mapping, inplace=True)
    return gdf

def clean_table_name(group_name):
    """
    Cleans and formats a table name so that they can be used in PostGIS database.

    Parameters:
    group_name (str): The column name to be cleaned.

    Returns:
    str: The cleaned table name ready for PostGIS.
    """
    # Function to clean and return a table name
    if group_name is None or group_name =='nan' or group_name == '':
        return 'unclassified'
    
    # Remove non-alphanumeric characters and white spaces
    cleaned_name = re.sub(r'[^\w\s]', '', str(group_name)).replace(' ', '_')   

    # Shorten the name
    if len(cleaned_name) > 40:
        cleaned_name = cleaned_name[:40]

    return f'{cleaned_name}'