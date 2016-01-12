__author__ = 'tmy'

import os
from datetime import datetime
from multiprocessing import Process
from .ProcessManager.ProcessManager import ProcessManager
from .NTripleLineParser.NTripleLineParser.NTripleLineParser import NTripleLineParser
from .SparqlInterface.src import ClientFactory
from .Materializer.Materializer import materialize_to_file, materialize_to_service
from .Utilities.Logger import log
from .Utilities.Utilities import log_progress


class SubClassReasoner(object):
    def __init__(self, server, user, password, prop_path, n_processes, log_level):
        log.setLevel(log_level)
        self.prop_path = prop_path
        self.nt_parser = NTripleLineParser(" ")
        self.processManager = ProcessManager(n_processes)
        self.__server = ClientFactory.make_client(server=server, user=user, password=password, prop_path=prop_path)

    def reason(self, file=None, target="./reasoned/", in_service=False):
        if in_service:
            target = None
        else:
            # Make directory
            if not os.path.exists(target):
                os.makedirs(target)

        if file:
            log.info("Reasoning from file")
            self.__reason_from_file(file, target)
        else:
            log.info("Reasoning from service")
            self.__reason_from_service(target)

    def __reason_from_service(self, target):
        cur_time = datetime.now()

        if target:
            target_file = target + str(self.__server.server).split("/")[-2] + str("_reasoned.nt")
        rdf_classes = self.__server.query(
            """
        SELECT distinct ?type
        WHERE {?type rdfs:subClassOf ?x}
        """)
        for i, t in enumerate(rdf_classes):
            log_progress(i, 100)
            t = t["type"]["value"]
            if target:
                self.__spawn_daemon(materialize_to_file, dict(rdf_type=t, target=target_file,
                                                              server=self.__server))
            else:
                self.__spawn_daemon(materialize_to_service, dict(rdf_type=t, server=self.__server))
        log.info("Done in: " + str(datetime.now() - cur_time))

    def __reason_from_file(self, f, target):
        cur_time = datetime.now()

        if target:
            target_file = target + str(f).split("/")[-1][:-3] + str("_reasoned.nt")
        # Iterate through file
        with open(f) as input_file:
            tmp_subject = ""
            for line_num, line in enumerate(input_file):
                subject = self.nt_parser.get_subject(line)
                if not subject:
                    continue
                log_progress(line_num, 100)

                if not subject == tmp_subject:
                    if target:
                        self.__spawn_daemon(materialize_to_file, dict(rdf_type=t, target=target_file,
                                                                      server=self.__server))
                    else:
                        self.__spawn_daemon(materialize_to_service, dict(rdf_type=t, server=self.__server))
        log.info("Done in: " + str(datetime.now() - cur_time))

    def __spawn_daemon(self, target, kwargs):
        # Todo Event based?
        # Check every 0.1 seconds if we can continue
        # while not self.processManager.has_free_process_slot():
        # time.sleep(0.1)

        p = Process(target=target, kwargs=kwargs)
        p.daemon = True
        p.start()

