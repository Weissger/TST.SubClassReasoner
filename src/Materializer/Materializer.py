__author__ = 'tmy'

from src.SparqlInterface.src.Interfaces.AbstractClient import SparqlConnectionError
from src.Utilities.Logger import log


def materialize_to_file(rdf_type=None, target=None, server=None):
    parents = __get_all_parents(rdf_type, server)
    with open(target, "a+") as f:
        for parent in parents:
            f.write("<" + rdf_type + "> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <" + parent + ">.\n")


def materialize_to_service(rdf_type=None, server=None):
    parents = __get_all_parents(rdf_type, server)
    for parent in parents:
        server.insert_triple("<" + rdf_type + ">", "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
                             "<" + parent + ">")


def __get_all_parents(rdf_type, server):
    parents = []
    retries = 0
    while True:
        try:
            parents = server.get_all_class_parents(rdf_type)
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
    return parents