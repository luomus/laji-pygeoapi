import pandas as pd
import numpy as np

def compute_coordinate_uncertainty(fb_occurrence_df):
    # Assuming the attributes are stored in a dictionary attached to the DataFrame
    dwc = fb_occurrence_df.attrs.get("dwc", True)
    vtype = col_type_string(dwc)

    vnms = sysdata("var_names")

    uncert_var = vnms.get(("computed_var_coordinates_uncertainty", vtype))

    add = fb_occurrence_df.attrs.get("include_new_cols", True)

    if add and uncert_var in fb_occurrence_df.columns:
        interp_var = vnms.get(("gathering.interpretations.coordinateAccuracy", vtype))
        interp = fb_occurrence_df[interp_var]

        source_var = vnms.get(("document.sourceId", vtype))
        source = fb_occurrence_df[source_var]

        na = (source == "http://tun.fi/KE.3") & (interp == 1)

        coord_uncert = np.where(na, np.nan, interp)

        fb_occurrence_df[uncert_var] = pd.to_numeric(coord_uncert, errors='coerce')

    return fb_occurrence_df

# Helper functions to mimic the R code behavior
def col_type_string(dwc):
    # Implement this function based on the specific logic of col_type_string in R
    pass

def sysdata(name):
    # Implement this function to load system data equivalent to sysdata in R
    pass
