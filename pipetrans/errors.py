# -*- coding: utf-8 -*-


class PipeTransError(Exception):
    """PipeTrans base error"""


class PipelineError(PipeTransError):
    """pipeline error"""


class OperatorError(PipeTransError):
    """command operator error"""

    def __init__(self, message, command, operator):
        super(OperatorError, self).__init__(message)
        self.command = command
        self.operator = operator


class CommandError(PipeTransError):
    """pipe command error"""

    def __init__(self, message, command):
        super(CommandError, self).__init__(message)
        self.command = command
