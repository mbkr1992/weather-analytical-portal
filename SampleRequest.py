from ecmwfapi import ECMWFDataServer

server = ECMWFDataServer()
server.retrieve({
    "class": "mc",
    "dataset": "cams_nrealtime",
    "date": "2017-10-01/to/2017-10-25",
    "expver": "0001",
    "levtype": "sfc",
    "param": "167.128",
    "step": "0",
    "stream": "oper",
    "time": "00:00:00",
    "type": "an",
    "target": "output",
})

print(server)