# -*- coding: utf-8 -*-

import copy
from pipetrans.commands import CommandFactory
from pipetrans.errors import PipelineError


class Pipeline(object):

    def __init__(self, pipeline, schema):
        pipeline = copy.deepcopy(pipeline)
        self.cmd = None
        prev_cmd = None
        for p in pipeline:
            cmd = CommandFactory.new(p)
            if prev_cmd:
                prev_cmd.next = cmd
            else:
                self.cmd = cmd
            prev_cmd = cmd

        if not self.cmd:
            raise PipelineError('pipeline specification must be an array of at least one command')

    def build_es(self):
        return {}

    def export_es(self):
        es_commands = self.build_es()
        return self.cmd.export_es(es_commands)
