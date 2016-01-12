from src import ProcessManager
from src.RemoteSparql import RequestsRemoteSparql, PymanticRemoteSparql

__author__ = 'tmy'

import os
from datetime import datetime
from multiprocessing import Process

from .src.RemoteSparql.PymanticRemoteSparql import PymanticRemoteSparql
from .src.Worker.Worker import materialize_reason
from .NtripleParser.NtripleParser import NtripleParser
from .src.Utilities.Logger import log
from .src.Utilities.Utilities import log_progress


PYMANTIC_SUPPORTED_ENDPOINTS = []


class SubClassReasoner(object):
    def __init__(self, server, user, password, prop_path, n_processes, log_level):
        log.setLevel(log_level)
        self.prop_path = prop_path
        self.subjectParser = NtripleParser(" ")
        self.processManager = ProcessManager(n_processes)

        # Create server instance
        if any([x in server for x in PYMANTIC_SUPPORTED_ENDPOINTS]):
            self.__server = PymanticRemoteSparql(server)
        else:
            self.__server = RequestsRemoteSparql(server, user, password)

    def reason(self, file, target):
        # Make directory
        if not os.path.exists(target):
            os.makedirs(target)

        # Iterate through file
        with open(file) as input_file:
            target_file = target + str(file).split("/")[-1][:-3] + str("_reasoned.nt")
            tmp_subject = ""
            cur_time = datetime.now()
            for line_num, line in enumerate(input_file):
                subject = self.subjectParser.parse_subject(line)
                if not subject:
                    continue
                log_progress(line_num, 100)

                # Todo Event based?
                # Check every 0.1 seconds if we can continue
                # while not self.processManager.has_free_process_slot():
                #     time.sleep(0.1)

                if not subject == tmp_subject:
                    tmp_subject = subject
                    p = Process(target=materialize_reason, kwargs=dict(rdf_type=subject, target=target_file,
                                                                       server=self.__server,
                                                                       prop_path=self.prop_path))
                    p.daemon = True
                    # try:
                    #     self.processManager.add(p)
                    # except OccupiedError as e:
                    #     return 2
                    # else:
                    p.start()
        log.info("Done in: " + str(datetime.now() - cur_time))

