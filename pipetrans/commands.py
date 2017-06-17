# -*- coding: utf-8 -*-
from pipetrans.errors import PipelineError, CommandError
from pipetrans.operator import OperatorFactory


_commands = {}


class CommandFactory(object):

    @staticmethod
    def new(value):
        if not isinstance(value, dict):
            raise PipelineError("pipeline element is not an object")
        if len(value) != 1:
            raise PipelineError("pipeline specification object must contain exactly one field")
        cmd_k = value.keys()[0]
        cmd_v = value[cmd_k]
        if cmd_k in _commands:
            return _commands[cmd_k](cmd_v)
        else:
            raise PipelineError("unknow pipeline command '%s'" % cmd_k)


class CommandMeta(type):

    def __init__(cls, name, bases, attrs):
        command = getattr(cls, "name", None)
        if command and command not in _commands:
            _commands[command] = cls
        super(CommandMeta, cls).__init__(name, bases, attrs)


class Command(object):

    __metaclass__ = CommandMeta

    def __init__(self, value):
        self.value = value
        self.next = None
        self.documents = []

    def make_error(self, message):
        return CommandError(message, self.name)

    def export_es(self, es_commands):
        for op in self.operators:
            es_commands = op.export_es(es_commands)
        if self.next:
            return self.next.export_es(es_commands)
        else:
            return es_commands


class MatchCommand(Command):

    name = "$match"

    def __init__(self, value):
        super(MatchCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("$match specification must be an object")
        operators = []
        for k, v in value.iteritems():
            operators.append(OperatorFactory.new_match(k, v))
        self.operators = operators

    def build_es(self, es_commands):
        if not es_commands.get('query'):
            es_commands['query'] = {
                'bool': {
                    'must': []
                }
            }
        return es_commands

    def export_es(self, es_commands):
        es_commands = self.build_es(es_commands)
        return super(MatchCommand, self).export_es(es_commands)


class GroupCommand(Command):

    name = "$group"

    def __init__(self, value):
        super(GroupCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("$group specification must be an object")
        elif "_id" not in value:
            raise self.make_error("$group specification must include an _id")
        operators = []
        for k, v in value.iteritems():
            if k == "_id":
                for _k, _v in v.iteritems():
                    operators.append(OperatorFactory.new_group(_k, _v))
            else:
                operators.append(OperatorFactory.new_group(k, v))
        self.operators = operators

    def build_es(self, es_commands):
        if not es_commands.get('aggs'):
            es_commands['aggs'] = {}
        return es_commands

    def export_es(self, es_commands):
        es_commands = self.build_es(es_commands)
        return super(GroupCommand, self).export_es(es_commands)
