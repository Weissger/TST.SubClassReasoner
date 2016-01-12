from src.RemoteSparql import AbstractRemoteSparql

__author__ = 'tmy'

from src.RemoteSparql.AbstractRemoteSparql import SparqlConnectionError
from src.Utilities.Logger import log


def materialize_reason(rdf_type=None, target=None, server=AbstractRemoteSparql, prop_path=False):
    with open(target, "a+") as f:
        parents = []
        retries = 0
        while True:
            try:
                if prop_path:
                    parents = server.get_parents_ppath(rdf_type)
                else:
                    parents = server.get_parents_recursive(rdf_type)
            except SparqlConnectionError as e:
                if retries == 0:
                    log.warn("Error on query for: " + str(rdf_type) + "\n" + str(e) + "\n")
                retries += 1
                log.debug("Error on: " + str(rdf_type) + " - try number: " + str(retries) + "\n" + str(e) + "\n")
                if retries > 5:
                    break
            else:
                if retries > 0:
                    log.info("Success on retry for:" + str(rdf_type) + "\n")
                break
        for parent in parents:
            f.write("<" + rdf_type + "> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <" + parent + ">.\n")