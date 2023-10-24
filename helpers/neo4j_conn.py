"""Graph database connector and query parsers"""
from __future__ import annotations

from typing import Any, Dict, List
import pandas as pd

import neo4j
from neo4j.exceptions import CypherSyntaxError

# Import local modules
from helpers.utilities import *

# Logger import and configuration
from helpers.logger import logger, configure
configure("graph_connector_log.txt")

class Neo4jGraph:
    """Neo4j wrapper for graph operations."""

    def __init__(
        self, 
        url: str, 
        username: str, 
        password: str, 
        database: str = "neo4j",
        ) -> None:
        """Create a new Neo4j graph wrapper instance."""
        self._driver = neo4j.GraphDatabase.driver(url,
                                                   auth=(username, password))
        self._database = database
        self.schema = ""

        # Verify connection
        try:
            self._driver.verify_connectivity()
        except neo4j.exceptions.ServiceUnavailable:
            raise ValueError(
                "Could not connect to Neo4j database. "
                "Please ensure that the url is correct"
            )
        except neo4j.exceptions.AuthError:
            raise ValueError(
                "Could not connect to Neo4j database. "
                "Please ensure that the username and password are correct"
            )
        
    def close(self):
        """Closes the Neo4j connection."""
        if self.driver is not None:
            self.driver.close()
    
    #### Methods to query KG & output in various formats ####
    
    def query(self, 
              cypher_query: str, 
              params: dict = {}
              ) -> List[Dict[str, Any]]:
        """Query Neo4j database."""
        logger.debug(cypher_query)

        with self._driver.session(database=self._database) as session:
            try:
                data = session.run(cypher_query, params)
                return [r.data() for r in data]
            except CypherSyntaxError as e:
                raise ValueError(
                    "Generated Cypher Statement is not valid\n" f"{e}") 
            
    def query_to_list(self, 
                      cypher_query: str,
                      params: dict = {}
                      ) -> List[Dict[str, Any]]:
        """Parses a Cypher statement and returns the output formatted as a list."""
        logger.debug(cypher_query)
        with self._driver.session(database=self._database) as session:
            data = session.run(cypher_query, params)
            return [r.values()[0] for r in data]
    
    def query_to_df(self, 
                    cypher_query: str,
                    params: dict = {}
                    ) -> pd.DataFrame:
        '''
        Parses a Cypher statement and returns the output as a pandas dataframe.
        '''
        logger.debug(cypher_query)
        with self._driver.session(database=self._database) as session:
            data = session.run(cypher_query, params)
            return pd.DataFrame([r.values() for r in data],
                                columns=data.keys())
    
        
        
    
        
    