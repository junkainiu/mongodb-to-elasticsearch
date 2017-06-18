from pipetrans import pipetrans
import unittest

class TestPipeline(unittest.TestCase):

    def test_no_schema(self):
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
                            "ts": "$ts",
                            "component": "$client_component",
                            "server_ip": "$server_ip"
                        },
                        "client_pktlen": {"$sum": "$client_pktlen"},
                        "server_pktlen": {"$sum": "$server_pktlen"}
                    }
                }
        ]
        schema = {}
        es_pipe = pipetrans(mongo_pipe, schema)
        expect = {
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
            "aggs": {
              "component": {
                "terms": {
                  "field": "client_component"
                }
              },
              "aggs": {
                "ts": {
                  "terms": {
                    "field": "ts"
                  }
                },
                "aggs": {
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
                  },
                  "server_ip": {
                    "terms": {
                      "field": "server_ip"
                    }
                  }
                }
              }
            }
          }
        }
        self.assertEqual(es_pipe, expect)

    def test_schema(self):
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
            'order': ['component', 'server_ip', 'ts']
        }
        es_pipe = pipetrans(mongo_pipe, schema)
        expect = {
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
            "aggs": {
              "component": {
                "terms": {
                  "field": "client_component"
                }
              },
              "aggs": {
                "aggs": {
                  "ts": {
                    "date_histogram": {
                      "field": "ts",
                      "format": "epoch_second"
                    }
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
                },
                "server_ip": {
                  "terms": {
                    "field": "server_ip"
                  }
                }
              }
            }
          }
        }
        self.assertEqual(es_pipe, expect)
        
if __name__ == '__main__':
    unittest.main()
