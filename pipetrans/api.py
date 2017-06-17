# -*- coding: utf-8 -*-

from pipetrans.pipeline import Pipeline


def pipetrans(pipeline, schema=None):
    if not schema:
        schema = {}
    p = Pipeline(pipeline, schema)
    return p.export_es()
