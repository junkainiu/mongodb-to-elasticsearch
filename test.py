from pipetrans import pipetrans

mongo_pipe = [
        {
            "$match": {
                "site": {"$in": ["sh1", "sh2"]},
                'ts': {
                    '$gte': 1000,
                    '$lt': 500
                },
                "server_ip": "144.7.22.11"
            }
        },
        {
            "$group": {
                "_id": {
                    "ts": "$ts",
                    "component": "$client_component",
                    "server_ip": "$server_ip"
                },
                "client_pktlen": {"$sum": "$client_pktlen"},
                "server_pktlen": {"$sum": "$server_pktlen"}
            }
        }
]

schema = {
    'properties': {
        'ts': {
            'type': 'date',
            'format': 'epoch_second'
        }
    },
    'order': ['ts', 'component', 'server_ip']
}

xx = pipetrans(mongo_pipe, schema)
import json
print json.dumps(xx, indent=2)
import sys;sys.stdout=sys.stderr;import pdb;pdb.set_trace()
