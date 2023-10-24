import json
from typing import Any, List, Dict
import pickle
from itertools import combinations
import random

def write_json(an_object: List[Any], file_path: str ) -> None:
    """Writes a Python object to a json file."""
   
    with open(file_path, "w") as fp:
        json.dump(an_object, fp)


def read_json(file_path: str) -> Any:
    """Reads a json file to the Python object it contains."""

    with open(file_path, 'rb') as fp:
        return json.load(fp)
    
def flatten_list(nested_list: List[List]) -> List[str]:
    """Flattens a nested Python list."""
    flat_list = []
    for sublist in nested_list:
        flat_list.extend(sublist)
    return flat_list

def write_pkl(an_object: Any, file_path: str) -> None:
    """Writes a Python object to a pickle file."""
    with open(file_path, 'wb') as f:
        pickle.dump(an_object, f)

def read_pkl(an_object: Any, file_path: str) -> Any:
    """Reads a pickle file."""
    with open(file_path, 'rb') as f:
        an_object = pickle.load(f)
        return an_object
    
def extract_subdict(my_dict: Dict, 
                    keys_to_extract: List[str]
                    )-> Dict:
    """
    Extract a subset as a dictionary.
    
    Parameters:
    my_dict(dict): The dictionary from which to extract values.
    keys_to_extract (list): A list of keys to extract.
    
    Returns:
    dict: A dictionary that only includes the keys in keys_to_extract and their corresponding values.
    """

    return {key: my_dict[key] for key in keys_to_extract if key in my_dict}

def random_properties(nlist, used_pairs=[]):
    """From a list of sublists, extract random pairs.
    """
    grouped_elements = {}

    for sublist in nlist:
        if sublist[0] not in grouped_elements:
            grouped_elements[sublist[0]] = []
        grouped_elements[sublist[0]].append(sublist)

    pairs = []
    for key, values in grouped_elements.items():
        if len(values)>1:
            pairs.extend(combinations(values,2))
        
    # Remove pairs that have already been used
    pairs = [pair for pair in pairs if pair not in used_pairs]

    if len(pairs) > 4:
        chosen_pair = random.choice(pairs)
        used_pairs.append(chosen_pair)
        result = [chosen_pair[0][0], chosen_pair[0][1], 
                  chosen_pair[0][2], chosen_pair[1][1],
                  chosen_pair[1][2]]
        return result
    else:
        return None


