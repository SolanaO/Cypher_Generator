"""Functions to extract KG information."""

from __future__ import annotations

from typing import Any, List, Iterable
import pandas as pd

# Import local modules
from helpers.utilities import *
from helpers.neo4j_conn import Neo4jGraph

# Logger import and configuration
from helpers.logger import logger#, configure
#configure('graph_connector_log.txt')

#### Queries ####

node_properties_query = """
    CALL apoc.meta.data()
    YIELD label, other, elementType, type, property
    WHERE NOT type = "RELATIONSHIP" AND elementType = "node"
    WITH label AS nodeLabels, collect(property) AS properties
    RETURN {labels:nodeLabels, properties:properties} AS output
    """
rel_query = """
    CALL apoc.meta.data()
    YIELD label, other, elementType, type, property
    WHERE type = "RELATIONSHIP" AND elementType = "node"
    RETURN "(:" + label + ")-[:" + property + "]->(:" + toString(other[0]) + ")" AS output
    """
rel_properties_query = """
    CALL apoc.meta.data()
    YIELD label, other, elementType, type, property
    WHERE NOT type = "RELATIONSHIP" AND elementType = "relationship"
    WITH label AS nodeLabels, collect(property) AS properties
    RETURN {type: nodeLabels, properties: properties} AS output
    """
node_properties_with_types_query = """
    CALL apoc.meta.data()
    YIELD label, other, elementType, type, property
    WHERE NOT type = "RELATIONSHIP" AND elementType = "node"
    WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
    RETURN {labels: nodeLabels, properties: properties} AS output
    """
rel_properties_with_types_query = """
    CALL apoc.meta.data()
    YIELD label, other, elementType, type, property
    WHERE NOT type = "RELATIONSHIP" AND elementType = "relationship"
    WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
    RETURN {type: nodeLabels, properties: properties} AS output
    """
node_labels_query = """
    CALL apoc.meta.data()
    YIELD label, other, elementType, type, property
    WHERE NOT type = "RELATIONSHIP" AND elementType = "node"
    WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
    RETURN {labels: nodeLabels} AS output
    """

#### Extract KG Information through Neo4j Utilities ####

class Neo4jSchema(Neo4jGraph):
    """Neo4j wrapper for graph operations."""

    def __init__(
        self, 
        url: str, 
        username: str, 
        password: str, 
        ) -> None:
        """Create a new Neo4j graph wrapper instance and extract schema information."""

        self.url=url
        self.username=username
        self.password=password
        self.conn = Neo4jGraph(url, username, password)
    
    #### Schema Utilities ####
    
    def build_string_schema_simplified(self) -> str:
        """
        Build KG schema as a string. Properties data types not included.
        """
        node_properties = self.conn.query(node_properties_query)
        relationships = self.conn.query(rel_query)
        relationships_properties = self.conn.query(rel_properties_query)
    
        schema = f"""
        Node properties are the following:
        {[el['output'] for el in node_properties]}
        The relationships are the following:
        {[el['output'] for el in relationships]}
        Relationship properties are the following:
        {[el['output'] for el in relationships_properties]}
        """
        return schema
    
    def save_schema_simplified(self, 
                    schema_file: str
                    ) -> None:
        
        """Save simplified schema to a json file.""" 
        node_properties = self.conn.query(node_properties_query)
        relationships = self.conn.query(rel_query)
        relationships_properties = self.conn.query(rel_properties_query)

        node_props = [el['output'] for el in node_properties]
        rels = [el['output'] for el in relationships]
        rels_props = [el['output'] for el in relationships_properties]

        if None in node_props:
            logger.warning("There are None entries in the list of nodes.")
            node_props = [e for e in node_props if e is not None] 

        if None in rels:
            logger.warning("There are None entries in the relationships list.")
            rels = [e for e in rels if e is not None] 

        if None in rels_props:
            logger.warning("There are None entries in the relationships properties list.")
            rels_props = [e for e in rels_props if e is not None] 
   
        schema_info = {}
        schema_info['node_properties'] = node_props
        schema_info['relationships'] = rels
        schema_info['relationships_properties'] = rels_props

        write_json(schema_info, schema_file)


    def build_string_schema_full(self) -> str:
        """
        Build KG schema as a string. Properties data types included.
        """

        node_properties = self.conn.query(node_properties_with_types_query)
        relationships_properties = self.conn.query(rel_properties_with_types_query)
        relationships = self.conn.query(rel_query)
        
        schema = f"""
        Node properties are the following:
        {[el['output'] for el in node_properties]}
        Relationship properties are the following:
        {[el['output'] for el in relationships_properties]}
        The relationships are the following:
        {[el['output'] for el in relationships]}
        """
        return schema

    def save_schema_full(self, 
                    schema_file: str
                    ) -> None:
        """Save full schema to a json file.""" 
        node_properties = self.conn.query(node_properties_with_types_query)
        relationships = self.conn.query(rel_query)
        relationships_properties = self.conn.query(rel_properties_with_types_query)
       
        node_props = [el['output'] for el in node_properties]
        rels = [el['output'] for el in relationships]
        rels_props = [el['output'] for el in relationships_properties]

        if None in node_props:
            logger.warning("There are None entries in the list of nodes.")
            node_props = [e for e in node_props if e is not None] 

        if None in rels:
            logger.warning("There are None entries in the relationships list.")
            rels = [e for e in rels if e is not None] 

        if None in rels_props:
            logger.warning("There are None entries in the relationships properties list.")
            rels_props = [e for e in rels_props if e is not None] 
   
        schema_info = {}
        schema_info['node_properties'] = node_props
        schema_info['relationships'] = rels
        schema_info['relationships_properties'] = rels_props

        write_json(schema_info, schema_file)

    
    #### Nodes Utilities ####

    def node_properties_with_types(self, 
                                   node_props_file: str
                                   ) -> None:
        """
        Extracts node properties with data types and saves them to a file."""

        node_properties = self.conn.query(node_properties_with_types_query)
        node_props = [el['output'] for el in node_properties]
        node_props = [e for e in node_props if e is not None] 
        write_json(node_props, node_props_file)
    
    
    def get_node_names(self, 
                       nodes_file: str
                       ) -> None:
        
        """Extract and save node labels to a file."""

        nodes =  self.conn.query(node_labels_query)
        nodes = [el['output'] for el in nodes]
        _all_nodes = [el['labels'] for el in nodes]
        write_json(_all_nodes, nodes_file)
    
    def get_node_properties(self,
                            node_label: str,
                            ) -> List[str]:
        """Function to extract a list of properties for a given node."""

        cypher_query = f"""MATCH (c:{node_label}) RETURN keys(c) LIMIT 1;"""
        
        data = self.conn.query_to_df(cypher_query)
        return list(data.iloc[0])[0]
    
 
    ####  Relationships Utilities ####

    def relationship_properties_with_types(self, 
                                           rels_props_file: str
                                           ) -> None:

        """Extract relationships with properties and types and save them to a file."""
        
        relationships_properties = self.conn.query(rel_properties_with_types_query)
        rels_props = [el['output'] for el in relationships_properties]
        rels_props = [e for e in rels_props if e is not None]
        write_json(rels_props, rels_props_file)
    
    def relationships(self, 
                      rels_file: str
                      ) -> Iterable[str]:
        
        """Extract relationships with source and target nodes and save them to a file."""

        relationships = self.conn.query(rel_query)
        rels = [el['output'] for el in relationships]
        rels = [e for e in rels if e is not None] 
        write_json(rels, rels_file)


    #### Instances Utilities ####
    
    def extract_data_values(self, 
                            selected_labels: str, 
                            n: int) -> List[Any]:
        """
        Function to extract node instances, i.e. properties & their values.
        """
        extracted = []
        for label in selected_labels:
            query_vals = f"""MATCH (p:{label}) 
                            WITH p LIMIT {n}
                            RETURN p AS Node
                            """
            
            data = self.conn.query(query_vals)
            for entry in data:
                entry["Label"] = label
            extracted.append(data)
        
        return extracted
    
    def extract_relationship_instances( self,
                            rtriple: List[str], 
                            n: int,
                            ) -> List[Any]:
        """
        Function to extract instances for a given relationship, written as a triple.
        """
        extracted = []
        query_rels = f"""MATCH p=(a:{rtriple[0]})-[r:{rtriple[1]}]->(b:{rtriple[2]}) RETURN p LIMIT {n}"""
            
        data = self.conn.query(query_rels)
        for entry in data:
            extracted.append(entry["p"])
        return extracted
    
    def extract_multiple_relationships_instances( self,
                            rtriples: List[Any], 
                            n: int,
                            ) -> List[Any]:
        """
        Function to extract n instances for each relationship from a given list.
        """
        extracted = []
        for rtriple in rtriples:
            temp_list = self.extract_relationship_instances(rtriple, n)
            extracted.append(temp_list)
        return extracted

    def upload_graph_data(self,
                          node: str
                          ) -> pd.DataFrame:
        
        """Function to retrieve from KG all the data for a given node, keys attributes and their values for all node instances."""
        
        # Get a list of the node's attributes
        properties = self.get_node_properties(node)
        # A list to store all the data       
        all_properties = []
        # Retrieve one property at a time
        for prop in properties:
            cypher_query = f'''MATCH (n:{node})
                            RETURN n.{prop} AS {prop};'''
            values = self.conn.query(cypher_query)
            all_properties.append(values)

        # Flatten dictionaries in sublists
        flat_all = [[k for d in sublist for k in d.items()] for sublist in all_properties]
        # Transpose the list of lists and make each pair of key-value pairs a dictionary
        glist = [dict(pair) for pair in zip(*flat_all)]
        # Rewrite as pandas data frame
        df = pd.DataFrame(glist)
        return df

    
   
        
  

   

    

    



        
