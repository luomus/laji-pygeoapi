from datetime import date
import re

def clear_collections_from_config(pygeoapi_config, pygeoapi_config_out):
    """
    Deletes collection information from the pygeoapi configuration file and adds latest update date.

    Parameters:
    pygeoapi_config (str): The path to the pygeoapi configuration file.
    """
    print("Deleting collection information from the pygeoapi config file...")
    with open(pygeoapi_config, 'r') as file:
        lines = file.readlines()

    # Find the index of the line containing the keyword "resources"
    keyword_index = None
    for i, line in enumerate(lines):

        # Add latest update timestamp to pygeoapi description
        match = re.search(r'\d{4}-\d{2}-\d{2}', line)
        if match:
            lines[i] = line.replace(match.group(), str(date.today()))

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