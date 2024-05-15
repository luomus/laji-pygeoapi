def clear_collections_from_config(pygeoapi_config_in, pygeoapi_config_out):
    """
    Deletes collection information from the pygeoapi configuration file.

    Parameters:
    pygeoapi_config (str): The path to the pygeoapi configuration file.
    """
    print("Deleting collection information from the pygeoapi config file...")
    with open(pygeoapi_config_in, 'r') as file:
        lines = file.readlines()

    # Find the index of the line containing the keyword "resources"
    keyword_index = None
    for i, line in enumerate(lines):
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
        print("Warning: Didn't remove any collections as the pygeoapi configuration file does not have resources section")


def add_to_pygeoapi_config(template_resource, template_params, input_config, output_config):
    """
    Adds information of the PostGIS tables to the pygeoapi configuration file. See https://docs.pygeoapi.io/en/latest/configuration.html#resources

    Parameters:
    template_resource (str): The path to the template file.
    template_params (dict): Dictionary containing placeholders and their corresponding real values.
    input_config (str): The path to the input pygeoapi configuration file.
    output_config (str): The path to the output pygeoapi configuration file.
    """

    # Read the input config file (i.e. config without any resources) and store its content
    with open(input_config, "r") as file:
        input_content = file.read()

    # Read the template file
    with open(template_resource, "r") as file:
        template = file.read()

    # Replace placeholders with real values
    for key, value in template_params.items():
        template = template.replace(key, value)

    # Write the input content followed by the filled template to the output config file
    with open(output_config, "w") as file:
        file.write(input_content)
        file.write(template)