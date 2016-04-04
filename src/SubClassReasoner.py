__author__ = 'tmy'

import os
from datetime import datetime
from multiprocessing import Process
from .ProcessManager.ProcessManager import ProcessManager, OccupiedError
from .NTripleLineParser.src.NTripleLineParser import NTripleLineParser
from .SparqlInterface.src import ClientFactory
from .Materializer.Materializer import materialize_to_file, materialize_to_service
from .Utilities.Logger import log
from .Utilities.Utilities import log_progress
import time


class SubClassReasoner(object):
    def __init__(self, server, user, password, prop_path, n_processes, log_level):
        log.setLevel(log_level)
        self.prop_path = prop_path
        self.nt_parser = NTripleLineParser(" ")
        if n_processes:
            self.processManager = ProcessManager(n_processes)
        self.__server = ClientFactory.make_client(server=server, user=user, password=password, prop_path=prop_path)

    def reason(self, in_file=None, target="./reasoned/", offset=0):
        if target == "":
            target = None
        else:
            # Make directory
            if not os.path.exists(target):
                os.makedirs(target)

        cur_time = datetime.now()
        if in_file:
            log.info("Reasoning from file")
            self.__reason_from_file(in_file, target, offset=offset)
        else:
            log.info("Reasoning from service")
            self.__reason_from_service(target, offset=offset)
        log.info("Done in: " + str(datetime.now() - cur_time))

    def __reason_from_service(self, target, offset=0):
        target_file = None
        step = 100000
        while True:
            rdf_classes = self.__server.query(
                """
            SELECT distinct ?type
            WHERE {{?type rdfs:subClassOf ?x}}
            ORDER BY ?type
            LIMIT {}
            OFFSET {}
            """.format(step, offset))
            log.debug("Number of Query results: {}".format(len(rdf_classes)))
            if len(rdf_classes) < 1:
                break
            log.debug("Step size: {} Offset: {} Starting_type: {}".format(step, offset, rdf_classes[0]["type"]["value"]))
            for t in rdf_classes:
                offset += 1
                log_progress(offset, 100)
                t = t["type"]["value"]
                if target:
                    if not target_file:
                        target_file = target + str(self.__server.server).split("/")[-2] + str("_reasoned.nt")
                    self.__spawn_daemon(materialize_to_file, dict(rdf_type=t, target=target_file,
                                                                  server=self.__server))
                else:
                    self.__spawn_daemon(materialize_to_service, dict(rdf_type=t, server=self.__server))

    def __reason_from_file(self, f, target, offset=0):
        target_file = None
        # Iterate through file
        with open(f) as input_file:
            tmp_type = ""
            for line_num, line in enumerate(input_file):
                t = self.nt_parser.get_subject(line)
                if not t:
                    offset += 1
                    continue
                if line_num < offset:
                    continue
                log_progress(line_num, 100)

                if not t == tmp_type:
                    if target:
                        if not target_file:
                            target_file = target + str(self.__server.server).split("/")[-2] + str("_reasoned.nt")
                        self.__spawn_daemon(materialize_to_file, dict(rdf_type=t, target=target_file,
                                                                      server=self.__server))
                    else:
                        self.__spawn_daemon(materialize_to_service, dict(rdf_type=t, server=self.__server))
                    tmp_type = t

    def __spawn_daemon(self, target, kwargs):
        # Todo Event based?
        # Check every 0.1 seconds if we can continue
        if hasattr(self, "processManager"):
            while not self.processManager.has_free_process_slot():
                time.sleep(0.1)

        p = Process(target=target, kwargs=kwargs)
        p.daemon = True
        if hasattr(self, "processManager"):
            try:
                self.processManager.add(p)
            except OccupiedError as e:
                log.critical(e)
                return 2
            else:
                p.start()
        else:
            p.start()

