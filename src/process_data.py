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

def convert_dates(dates):
    """
    Extracts and formats event dates from a GeoDataFrame.

    Parameters:
    dates (Pandas Dataframe): The (Geo)Series containing date information.

    Returns:
    tuple: A tuple containing the dates as pandas Series.
    """

    # Define the regex patterns to find days and times
    date_regex = '[0-9]{4}-[0-9]{2}-[0-9]{2}'
    time_regex = '[0-9]{2}:[0-9]{2}'
    time2_regex = '[0-9]{1}:[0-9]{2}'

    # Loop over dates and translate into the correct form
    for i, date in enumerate(dates):
        try:
            date = str(date)
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
    gdf (geopandas.GeoDataFrame): The GeoDataFrame containing column named eventDateTimeDisplay (or Aika).

    Returns:
    first_date: the first datestamp of the GeoDataframe in RFC3399 format
    end_date: the last datestamp of the Geodataframe in RFC3399 format
    """
    # Filter out NaT (Not a Time) values
    dates = gdf['Aika']
    dates = pd.to_datetime(dates)
    dates_without_na = dates.dropna()

    # Get the minimum and maximum dates in RFC3339 format
    if len(dates_without_na) > 0:
        first_date = str(dates_without_na.min().strftime('%Y-%m-%dT%H:%M:%SZ'))
        end_date = str(dates_without_na.max().strftime('%Y-%m-%dT%H:%M:%SZ'))
        return first_date, end_date
    else:
        return None, None
    
def combine_similar_columns(gdf):
    """
    Finds similar columns (e.g. keyword[0], keyword[1], keyword[2]) and combines them

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame similar columns with their names ending with [n]

    Returns:
    gdf (geopandas.GeoDataFrame): The geodataframe with combined columns
    """
    # Use regex to find columns with a pattern ending with [n]
    pattern = re.compile(r'^(.*)\[\d+\]$')
    
    # Dictionary to store the groups of columns
    columns_dict = {}

    for col in gdf.columns:
        match = pattern.match(col)
        if match:
            base_name = match.group(1)
            if base_name not in columns_dict:
                columns_dict[base_name] = []
            columns_dict[base_name].append(col)
    
    # Combine columns in each group
    for base_name, cols in columns_dict.items():
        gdf[base_name] = gdf[cols].apply(lambda row: ', '.join(row.dropna()), axis=1)
        gdf.drop(columns=cols, inplace=True)
    
    return gdf


def translate_column_names(gdf, lookup_table, style='virva'):
    """
    Maps column names in a GeoDataFrame to Darwin Core names using a lookup table.

    Parameters:
    gdf (geopandas.GeoDataFrame): The GeoDataFrame to be mapped.
    lookup_table (str): The path to the CSV lookup table.
    style (str): Format to column names tranlate to. Options: 'translated_var', 'dwc' and 'virva'. Defaults to 'virva'.

    Returns:
    geopandas.GeoDataFrame: The GeoDataFrame with columns renamed according to the lookup table.
    """
    # Load the lookup table CSV into a DataFrame
    lookup_df = pd.read_csv(lookup_table, sep=';', header=0)

    column_mapping = {}
    columns_to_remove = []

    # Iterate through the lookup table
    for _, row in lookup_df.iterrows():
        if len(str(row[style])) > 3:
            # Map columns if the given format exists
            column_mapping[row['finbif_api_var']] = row[style]
        else:
            # If there is nothing to map, add column to removable list
            columns_to_remove.append(row['finbif_api_var'])

    # Identify additional columns to drop (nan columns, columns with brackets)
    #for col in gdf.columns:
    #    if  '[' in str(col) and ']' in str(col):
            # TOOD: Combine all similar fields with brakcets 


    #    if (isinstance(col, float) or isinstance(col, int) or 
    #        (isinstance(col, str) and col.lower() == 'nan') or
    #        ('[' in str(col) and ']' in str(col)) or 
    #        (len(str(col)) < 4)):
    #        columns_to_remove.append(col)

    # Remove columns from gdf that do not have a corresponding variable in lookup_df
    gdf.drop(columns=columns_to_remove, inplace=True, errors='ignore')

    gdf = combine_similar_columns(gdf)

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