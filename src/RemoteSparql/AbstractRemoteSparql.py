__author__ = 'tmy'

import abc


class AbstractRemoteSparql(object):
    """
    Abstract class to implement a remote sparql interface.
    """
    metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_parents_ppath(self, rdf_type):
        pass

    @abc.abstractmethod
    def get_parents_recursive(self, rdf_type):
        pass


class SparqlConnectionError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "Service didn't answer with 200: " + str(self.value)