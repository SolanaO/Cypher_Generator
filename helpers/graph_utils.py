"""Functions to extract KG information."""

from __future__ import annotations

from typing import Any, Dict, List, Union
from Levenshtein import distance
import re
from neo4j import time

# Import local modules
from helpers.utilities import *

#### NODES ####

def extract_nodes_with_properties_of_specified_type(
                                                    node_props: List[str],selected_labels: List[str], data_type: str
                                                    ) -> List[Any]:
      
    """Given a list of node labels, extract the properties that have a specified data type.
    Data types: STRING, INTEGER, BOOLEAN, DATE, DATE_TIME, FLOAT
    """
    output = []
    
    for node in node_props:
        if node['labels'] in selected_labels:
            temp_dict = {}
            temp_dict['label']= node['labels']
            temp_dict['selected_properties'] = [prop['property'] for prop in node['properties'] if prop['type'] == data_type]
            output.append(temp_dict)
    return [entry for entry in output if entry["selected_properties"]]

    
#### RELATIONSHIPS ####

def extract_relationships_list(rels: str)-> List[str]:
    """Extract a list of relationships."""

    # Regular expression looks for any text enclosed in square brackets
    pattern = r'\[:(.*?)\]'

    # Use a set to store unique expressions
    expressions = set()

    for entry in rels:
        matches = re.findall(pattern, entry)
        for match in matches:
            expressions.add(match)
    return list(expressions)

    
def extract_relationships_triples(rels: str)-> List[Any]:
    """Extract triples of the form source node, relationship, target node."""

    # Regular expression looks for three groups of text separated by specific characters
    pattern = r'\(:(.*?)\)-\[:(.*?)\]->\(:(.*?)\)'

    # Store the triples
    triples = []

    for entry in rels:
        matches = re.findall(pattern, entry)
        for match in matches:
            triples.append(list(match))
    return triples

    
def extract_rels_with_properties_of_specified_type( 
                                rels_props: List[str], 
                                selected_rels: List[str], 
                                data_type: str
                                ) -> List[Any]:
        
    """Given a list of relationships, extract for each type the properties that have a specified data type.
    Available: STRING, INTEGER, BOOLEAN, DATE, DATE_TIME, FLOAT
    """
    output = []
    for rel in rels_props:
        if rel['type'] in selected_rels:
            temp_dict = {}
            temp_dict['type']= rel['type']
            temp_dict['selected_properties'] = [prop['property'] for prop in rel['properties'] if prop['type'] == data_type]
            output.append(temp_dict)
    return [entry for entry in output if entry["selected_properties"]]
    

#### INSTANCES ####

def neo4j_date_to_string(v: Any) -> str:
    """Convert neo4j.time.Date to ISO formatted string."""
    return f"{v.year}-{v.month:02d}-{v.day:02d}"

def neo4j_datetime_to_string(v: Any) -> str:
    """Convert neo4j.time.DateTime to ISO formatted string."""
    return f"{v.year}-{v.month:02d}-{v.day:02d}T{v.hour:02d}:{v.minute:02d}:{v.second:02d}"

def transform_dates_in_dict(d: Any)-> Dict:
    """
    Transform neo4j.time.Date objects in a dictionary to ISO formatted strings."""
    for key, value in d.items():
        if isinstance(value, time.Date):
            d[key] = neo4j_date_to_string(value)
    return d

def serialize_node_data(entries: List[Any]) -> List[str]:
    """Transform all date and datetime objects related to nodes into strings."""
    for sublist in entries:
        for instance in sublist:
            instance['Node'] = transform_dates_in_dict(instance['Node'])
            instance['Node'] = transform_datetime_in_dict(instance['Node'])
    return entries

def serialize_rels_data(entries: List[Any]) -> List[Any]:
    """Transform all date and datetime objects related to relationships into strings."""
    for sublist in entries:
        for instance in sublist:
            instance[0] = transform_dates_in_dict(instance[0])
            instance[0] = transform_datetime_in_dict(instance[0])
            instance[2] = transform_dates_in_dict(instance[2])
            instance[2] = transform_datetime_in_dict(instance[2])
    return entries

def transform_datetime_in_dict(d: Dict) -> Dict:
    """
    Transform neo4j.time.DateTime objects to a dictionary of ISO formatted strings."""
    for key, value in d.items():
        if isinstance(value, time.DateTime):
            d[key] = neo4j_datetime_to_string(value)
    return d
    
def parse_instances(nodes: List[str], 
                    node_props: List[str], 
                    data_type: str,
                    extracted_data_values: List[Any])->List[Any]:
    """Parse instances of nodes and properties with specified data type."""
    
    type_props = extract_nodes_with_properties_of_specified_type(node_props, nodes, data_type)

    parsed_result = []
    for instance in extracted_data_values:
        for inst in instance:
            label = inst["Label"]
            props = next((item['selected_properties'] for item in type_props
                          if item['label']==label), None)
            if props:
                for key, value in inst['Node'].items():
                    if key in props:
                        parsed_result.append([label, key, value])
    return parsed_result
    
    
def combine_rels_with_labels(triples_list: List[Any],
                             relationships_instances_list: List[Any]
                             )->List[Any]:
    """Adds node labels to relationships."""
    
    combined_list = []
    for i, triple in enumerate(triples_list):
        for instance in relationships_instances_list[i]:
            instance.insert(0, triple[0])
            instance[2] = triple[1]
            instance.append(triple[2])
            combined_list.append(instance)
    return combined_list
    
def filter_relationships_instances(node_props_types: List[Any],
                                   nodes_list: List[str],
                                   parsed_rels_instances: List[Any],data_type_source: str,
                                   data_type_target: str
                                   )-> List[Any]:
    """Parses a list of relationships. It extracts those properties for both source and target nodes that are of specified data types.
        
    NOTE: This will give errors if the instance does not have the required combination of source-target data types.
    """

    result = []
    dtype_properties_source = extract_nodes_with_properties_of_specified_type(node_props_types, nodes_list, data_type_source)
    dtype_properties_target = extract_nodes_with_properties_of_specified_type(node_props_types, nodes_list, data_type_target)

    for instance in parsed_rels_instances:
        label_source, properties_source = instance[0], instance[1]
        label_target, properties_target = instance[4], instance[3]
        relationship = instance[2]
        for entry in dtype_properties_target:
            if entry['label'] == label_target:
                selected_target = {prop: properties_target[prop] for prop in entry['selected_properties'] if prop in properties_target}
        for entry in dtype_properties_source:
            if entry['label'] == label_source:
                selected_source = {prop: properties_source[prop] for prop in entry['selected_properties'] if prop in properties_source}
                
        result.append([label_source, selected_source, relationship,
                label_target, selected_target])
    return result

def add_selected_properties(instances: List[Any], 
                            type_properties: List[str],
                            new_key: str
                            ) -> List[Any]:

    # Create a dictionary from parsed_properties with label as key and selected_list as values
    temp_dict = {item['label']: item['selected_properties'] for item in type_properties}

    # Iterate over the lists in instances_nodes
    for sublist in instances:
        for item in sublist:
            # If Label matches with a key in temp_dict, and the list is not empty, add the selected_properties
            if item['Label'] in temp_dict and temp_dict[item['Label']]:
                item[f'{new_key}'] = temp_dict[item['Label']]
    return instances
    

    #### SUBGRAPHS ####
    
def get_graph_neighborhood(entity: str, 
                           lev_dist: int,
                           schema_file: str,
                           ) -> Union[List[Any], List[Any], List[Any]]:
    """
    Extracts all node labels, their properties and corresponding relationships information that are at a certain Levenshtein distance from a given string or contain the given string. To speed up the process
    the schema file is used.
    """
    
    schema_info = read_json(schema_file)
    node_properties = schema_info['node_properties']
    relationships = schema_info['relationships']
    relationships_properties = schema_info['relationships_properties']
    
    # Get all the nodes, and their properties, that have similar labels to entity
    local_node_properties = [node for node in node_properties if (distance(entity, node['labels']) <= lev_dist )]
    #or (entity.lower() in node['labels'].lower())]

    # Get a list of similar node labels
    local_nodes = [node['labels'] for node in local_node_properties]

    # Get all relationships that involve at least one of the local_nodes
    local_relationships = [rel for rel in relationships if any(node in rel for node in local_nodes)]
        
    # Extract the local_relationship types
    pattern = r'\[:([^]]*)\]'
    local_relationships_types = [re.findall(pattern, string)[0] for string in local_relationships for match in re.findall(pattern, string)]
        
    # Extract the local relationships properties
    # Local relationships with no properties will not show up in this list
    local_relationships_properties = [rel for rel in relationships_properties if rel['type'] in local_relationships_types]

    # To get all the relations
    local_relationships = relationships
    local_relationships_properties = relationships_properties

    return local_node_properties, local_relationships, local_relationships_properties
    
def get_subgraph_schema(entities: List[str], 
                        lev_dist: int,
                        schema_file: str,
                        ) -> Union[List[Any], List[Any], List[Any]]:
    """
    Extracts all node labels, their properties and corresponding relationships information for all the entities given in a list. 
    """
    subgraph_nodes = [] 
    subgraph_edges = []
    subgraph_edges_properties = []

    for entity in entities:
        nodes, edges, edges_properties = get_graph_neighborhood(entity, lev_dist, schema_file)
        subgraph_nodes.append(nodes)
        subgraph_edges.append(edges)
        subgraph_edges_properties.append(edges_properties)
    
    subgraph_nodes = flatten_list(subgraph_nodes)
    subgraph_edges = flatten_list(subgraph_edges)
    subgraph_edges_properties = flatten_list(subgraph_edges_properties)

    return subgraph_nodes, subgraph_edges, subgraph_edges_properties
    

def build_subschema(entities: List[str],
                    lev_dist: int,
                    schema_file: str,
                    ) -> str:
    """
    Builds the Neo4j subgraph schema, without property types and
    formats it for the prompt template.
    """
    subgraph_nodes, subgraph_edges, subgraph_edges_properties = get_subgraph_schema(entities, lev_dist, schema_file)

    subschema = f"""
    Node properties are the following:
    {subgraph_nodes}
    The relations are the following:
    {subgraph_edges}
    Relations properties are the following:
    {subgraph_edges_properties}
    """
    return subschema


 
   

    




  

    
 
        
  

   

    

    



        
