import os
from pathlib import Path
import json 

import pandas as pd


# This package is intended to be installed along with the data folder
BASEPATH = Path(__file__).resolve().parent

_DEFAULT_INPUT_PATH = Path(BASEPATH).parent / "input_data"
_DEFAULT_OUTPUT_PATH = Path(BASEPATH).parent / "output_data"
INPUT_PATH = Path(os.environ.get("INPUT_DIR", _DEFAULT_INPUT_PATH))
OUTPUT_PATH = Path(os.environ.get("OUTPUT_DIR", _DEFAULT_OUTPUT_PATH))

_INPUT_DEFAULT_PATHS = dict(
    DE1="BW_Baden_Wuerttemberg",
    DE2="BY_Bayern",
    DE4="BR_Brandenburg",
    DE7="HE_Hessen",
    DE8="MP_Mecklenburg_Vorpommern",
    DE9="NiS_Niedersachsen",
    DEA="NRW_Nordrhein_Westfalen",
    DEB="RLP_Rheinland_Pfalz",
    DEC="SL_Saarland",
    DED="SN_Sachsen",
    DEE="SA_Sachsen_Anhalt",
    DEF="SH_Schleswig_Holstein",
    DEG="TH_Thueringen"
)

def get_input_path(bl: str):
    """
    Returns the input path for a given Bundesland, which is identified
    by its NUTS ID.
    
    """
    if bl in _INPUT_DEFAULT_PATHS:
        return Path(INPUT_PATH) / "Q_and_W" / _INPUT_DEFAULT_PATHS[bl]
    else:
        raise ValueError(f"No default path for Bundesland {bl} available.")
    
def get_output_path(camels_id: str):
    """
    Return the output path for a given Bundesland or Station, identified 
    by their NUTS ID.
    
    """
    if camels_id is None:
        return OUTPUT_PATH
    elif camels_id in _INPUT_DEFAULT_PATHS.keys():
        # output path of Bundesland
        return Path(OUTPUT_PATH / camels_id)
    elif len(camels_id) == 8: # TODO: check that camels_id is in metadata / nuts mapping?
        # output path of Station
        return Path(OUTPUT_PATH / camels_id[:3] / camels_id)
    else:
        raise ValueError(f"Given camels_id '{camels_id}' not recognized.")

def get_metadata1h():
    """
    Returns the metadata of all stations.
    
    """
    return pd.read_csv(Path(INPUT_PATH) / "metadata" / "metadata1h.csv")

def get_nuts_id_from_provider_id(provider_id: str, bl: str, add_missing: bool = False):
    """
    Returns the NUTS ID of a station given its provider ID.  
    If the provider ID is not found in the mapping, add_missing can be set to True to add 
    the missing mapping. For this, you need to provide the NUTS ID of the Bundesland.
    
    """
    # get nuts mapping
    mapping = pd.read_csv(OUTPUT_PATH / "metadata" / "nuts_mapping.csv")

    # make sure that provider_id is a string
    provider_id = str(provider_id)

    # check if provider_id is already in camelsp nuts mapping
    if provider_id in mapping.provider_id.values:
        return mapping.set_index("provider_id").loc[provider_id, "nuts_id"]
    elif add_missing:
        return update_nuts_mapping(provider_id, bl)
    else:
        raise ValueError(f"Provider ID {provider_id} not found in mapping.")
    
def update_nuts_mapping(provider_id: str, bl: str):
    """
    Updates the nuts_mapping.csv and nuts_mapping.json files with a new provider_id and generated nuts_id.

    """
    # Check that bl is a valid NUTS ID
    if bl not in _INPUT_DEFAULT_PATHS:
        raise ValueError(f"{bl} is not a valid NUTS ID.")
    
    # Load the current mapping
    csv_path = OUTPUT_PATH / "metadata" / "nuts_mapping.csv"
    json_path = OUTPUT_PATH / "metadata" / "nuts_mapping.json"
    
    mapping = pd.read_csv(csv_path)
        
    # Generate a new nuts_id
    bl_mapping = mapping[mapping.nuts_id.str.startswith(bl)]

    # Check if provider_id is already in the mapping for the given Bundesland
    if provider_id in mapping.provider_id.values:
        raise ValueError(f"provider_id {provider_id} is already in the mapping")
    
    # Generate a new nuts_id
    if bl_mapping.empty:
        new_nuts_id = f"{bl}1000"
    else:
        last_nuts_id = bl_mapping.nuts_id.max()
        new_nuts_id = f"{bl}{int(last_nuts_id[-5:]) + 10:05d}"
    
    # Add the new mapping
    new_entry = pd.DataFrame({"provider_id": [provider_id], "nuts_id": [new_nuts_id]})
    updated_mapping = pd.concat([mapping, new_entry], ignore_index=True).sort_values("nuts_id")
    
    # Save the updated mapping
    updated_mapping.to_csv(csv_path, index=False)
    
    # Update the JSON file
    with open(json_path, "w") as json_file:
        # sort by nuts_id
        json_file.seek(0)
        json.dump(sorted(updated_mapping.to_dict(orient="records"), key=lambda x: x["nuts_id"]), json_file, indent=4)

    return new_nuts_id

def get_nuts_mapping(format: str = "csv"):
    """
    Returns the nuts mapping as a pandas DataFrame. 
    The format can be either 'csv' or 'json'.
    
    """
    fname = OUTPUT_PATH / "metadata" / "nuts_mapping.json"

    # if nuts_mapping does not exist, create empty mapping
    if not os.path.exists(fname):
        raise FileNotFoundError(f"Can't find the nuts_mapping at {fname}, returning empty mapping.")
    else:
        with open(fname, 'r') as f:
            mapping = json.load(f)
    
    # return
    if format.lower() == 'json':
        return mapping
    elif format.lower() in ("csv", "df", "dataframe"):
        return pd.DataFrame(mapping)