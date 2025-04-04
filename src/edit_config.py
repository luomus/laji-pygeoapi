from datetime import date
import re

def clear_collections_from_config(pygeoapi_config, pygeoapi_config_out):
    """
    Deletes collection information from the pygeoapi configuration file and adds latest update date.

    Parameters:
    pygeoapi_config (str): The path to the pygeoapi configuration file.

    Returns:
    last_update (str): The date when database was last updated
    """
    print("Deleting collection information from the pygeoapi config file...")
    with open(pygeoapi_config, 'r') as file:
        lines = file.readlines()

    # Find the index of the line containing the keyword "resources"
    keyword_index = None
    for i, line in enumerate(lines):

        # Find the point where resources section starts
        if "resources" in line:
            keyword_index = i
            break

    # If the keyword is found, keep only the lines before it
    if keyword_index is not None:
        lines = lines[:keyword_index+1]

        # Write the modified contents back to the file
        with open(pygeoapi_config_out, 'w') as file:
            file.writelines(lines)
    else:
        print("Didn't remove any collections as the pygeoapi configuration file does not have resources section")


def add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config_out):
    """
    Adds information of the PostGIS tables to the pygeoapi configuration file. See https://docs.pygeoapi.io/en/latest/configuration.html#resources

    Parameters:
    template_resource (str): The path to the template file.
    template_params (dict): Dictionary containing placeholders and their corresponding real values.
    output_config (str): The path to the output pygeoapi configuration file.
    """

    # Read the template file
    with open(template_resource, "r") as file:
        template = file.read()

    # Replace placeholders with real values
    for key, value in template_params.items():
        template = template.replace(key, value)

    # Append the filled template to the output config file
    with open(pygeoapi_config_out, "a") as file:
        file.write(template)

def add_metadata_to_config(pygeoapi_config_out, db_path_in_config):
    """
    This function adds metadata information to the pygeoapi config file. 
    
    Parameters:
    pygeoapi_config_out (str): The path to the pygeoapi config file
    db_path_in_config (str): path to the tinydb catalogue that stores metadata information
    """
    config_template = f"""
    occurrence-metadata:
        type: collection
        title: Occurrence Metadata 
        description: This metadata record contains metadata of the all collections in this service 
        keywords:
            en:
                - metadata
                - record
        extents:
            spatial:
                bbox: [19.08317359,59.45414258,31.58672881,70.09229553]
                crs: https://www.opengis.net/def/crs/EPSG/0/3067
            temporal: 
                begin: 1990-01-01T00:00:00Z
                end: {str(date.today())}T00:00:00Z
        providers:
          - type: record
            name: TinyDBCatalogue
            data: {db_path_in_config}
            id_field: externalId
            time_field: recordCreated
            title_field: title
    """

    # Append the filled template to the output config file
    with open(pygeoapi_config_out, "a") as file:
        file.write(config_template)
        print(f"metadata ({db_path_in_config}) added to config file {pygeoapi_config_out}")
