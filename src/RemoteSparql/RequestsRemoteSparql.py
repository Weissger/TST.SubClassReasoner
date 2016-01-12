from src.RemoteSparql import AbstractRemoteSparql

__author__ = 'tmy'

import requests

from .src.RemoteSparql.AbstractRemoteSparql import SparqlConnectionError


class RequestsRemoteSparql(AbstractRemoteSparql):
    def __init__(self, server, user, password):
        self.session = requests.session()
        self.session.auth = (user, password)
        self.server = server

    def get_parents_recursive(self, rdf_type):
        query = """SELECT ?class WHERE {{
          <{}> rdfs:subClassOf ?class
        }}""".replace('\n', '')
        done_list = []
        more = [rdf_type]
        while True:
            if not more:
                break
            cur = more.pop()
            done_list.append(cur)
            result = self.session.post(self.server, params={"query": query.format(cur)},
                                  headers={"Accept": "application/sparql-results+json"}).json()
            for o in result["results"]["bindings"]:
                new_c = o["class"]["value"]
                if new_c not in done_list and new_c not in more:
                    more.append(new_c)
        return done_list

    def get_parents_ppath(self, rdf_type):
        query = """SELECT distinct ?class WHERE {{
          <{}> rdfs:subClassOf+ ?class
        }}""".replace('\n', '')
        done_list = []
        try:
            result = self.session.post(self.server, params={"query": query.format(rdf_type)},
                                   headers={"Accept": "application/sparql-results+json"})
        except Exception as e:
            print(e)
            raise(SparqlConnectionError(e))

        if result.status_code == 200:
            result = result.json()
        else:
            raise(SparqlConnectionError(result))
        for o in result["results"]["bindings"]:
            done_list.append(o["class"]["value"])
        return done_list