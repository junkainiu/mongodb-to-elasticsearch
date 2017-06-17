# -*- coding: utf-8 -*-

from pipetrans.errors import CommandError, OperatorError
from pipetrans.keys import BUCKETS_AGGREGATE_KEYS

_operators = {}


class OperatorFactory(object):

    @staticmethod
    def new_match(key, value):
        if not isinstance(value, dict):
            return MatchEqualOperator(key, value)
        else:
            if len(value) == 1:
                name = value.keys()[0]
                match_operators = _operators.get("$match", {})
                if name in match_operators:
                    return match_operators[name](key, value[name])
                else:
                    raise CommandError("unknow $match operator '%s'" % name, "$match")
            else:
                return MatchCombineOperator(key, value)

    @staticmethod
    def new_group(key, value):
        if not isinstance(value, dict):
            return GroupTermOperator(key, value)
        if len(value) == 1:
            name = value.keys()[0]
            group_operators = _operators.get("$group", {})
            if name in group_operators:
                return group_operators[name](key, value[name])
            else:
                raise CommandError("unknow $group operator '%s'" % name, "$group")
        else:
            raise CommandError("the computed aggregate '%s' must specify exactly one operator" % key)


class OperatorMeta(type):

    def __init__(cls, name, bases, attrs):
        command = getattr(cls, "command", None)
        operator = getattr(cls, "name", None)
        if command and operator:
            if command not in _operators:
                _operators[command] = {}
            _operators[command][operator] = cls
        super(OperatorMeta, cls).__init__(name, bases, attrs)


class Operator(object):

    __metaclass__ = OperatorMeta

    def make_error(self, message):
        return OperatorError(message, self.command, self.name)


class MatchOperator(Operator):

    command = "$match"


class MatchKeyOperator(MatchOperator):

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def export_es(self, es_commands):
        return self.build_es(es_commands)


class MatchEqualOperator(MatchKeyOperator):

    def build_es(self, es_commands):
        commands = {
            'bool': {
                'should': [
                    {'term': {self.key: self.value}}
                ]
            }
        }
        es_commands['query']['bool']['must'].append(commands)
        return es_commands


class MatchInOperator(MatchKeyOperator):

    name = "$in"

    def build_es(self, es_commands):
        commands = {
            'bool': {
                'should': [
                    {'term': {self.key: v}} for v in self.value
                ]
            }
        }
        es_commands['query']['bool']['must'].append(commands)
        return es_commands


class MatchCombineOperator(MatchKeyOperator):

    def __init__(self, key, value):
        super(MatchCombineOperator, self).__init__(key, value)
        combined_ops = []
        for k, v in value.iteritems():
            combined_ops.append(OperatorFactory.new_match(key, {k: v}))
        self.combined_ops = combined_ops

    def export_es(self, es_commands):
        for op in self.combined_ops:
            es_commands = op.export_es(es_commands)
            return es_commands


class GroupOperator(Operator):

    command = "$group"

    def __init__(self, key, value):
        self.key = key
        self.value = value[1:]

    def export_es(self, es_commands):
        return self.build_es(es_commands)


class GroupMetricOperator(GroupOperator):

    def __init__(self, key, value):
        self.oper = self.name[1:]
        super(GroupMetricOperator, self).__init__(key, value)


class GroupSumOperator(GroupMetricOperator):

    name = "$sum"

    def build_es(self, es_commands):
        commands = {
            self.key: {
                self.oper: {
                    "field": self.value
                }
            }
        }
        aggs = es_commands['aggs']
        while aggs.get('aggs'):
            aggs = aggs['aggs']
        if len(aggs) > 1:
            aggs.update(commands)
        elif len(aggs) == 1:
            if aggs.items()[0][1].keys()[0] in BUCKETS_AGGREGATE_KEYS:
                aggs['aggs'] = commands
            else:
                aggs.update(commands)
        else:
            aggs.update(commands)
        return es_commands


class GroupBucketOperator(GroupOperator):

    def __init__(self, key, value):
        self.oper = self.name
        super(GroupBucketOperator, self).__init__(key, value)


class GroupTermOperator(GroupBucketOperator):

    name = 'terms'

    def build_es(self, es_commands):
        commands = {
            self.key: {
                self.oper: {
                    'field': self.value
                }
            }
        }
        aggs = es_commands['aggs']
        while aggs.get('aggs'):
            aggs = aggs['aggs']
        aggs['aggs'] = commands
        return es_commands
