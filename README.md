# mongodb-to-elasticsearch
Transform mongo aggregate pipeline to Elasticsearch pipeline

## Exapmle
```
from pipetrans import pipetrans

mongo_pipe = [
        {
            "$match": {
                "site": {"$in": ["sh1", "sh2"]},
                "server_ip": "144.7.22.11"
            }
        },
        {
            "$group": {
                "_id": {
                    "component": "$client_component",
                    "server_ip": "$server_ip"
                },
                "client_pktlen": {"$sum": "$client_pktlen"},
                "server_pktlen": {"$sum": "$server_pktlen"}
            }
        }
]

pipetrans(mongo_pipe)

{
    "query": {
        "bool": {
            "must": [
                {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "site": "sh1"
                                }
                            },
                            {
                                "term": {
                                    "site": "sh2"
                                }
                            }
                        ]
                    }
                },
                {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "server_ip": "144.7.22.11"
                                }
                            }
                        ]
                    }
                }
            ]
        }
    },
    "aggs": {
        "component": {
            "terms": {
                "field": "client_component"
            },
            "aggs": {
                "ts": {
                    "terms": {
                        "field": "ts"
                    },
                    "aggs": {
                        "server_ip": {
                            "terms": {
                                "field": "server_ip"
                            },
                            "aggs": {
                                "server_pktlen": {
                                    "sum": {
                                        "field": "server_pktlen"
                                    }
                                },
                                "client_pktlen": {
                                    "sum": {
                                        "field": "client_pktlen"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

```
