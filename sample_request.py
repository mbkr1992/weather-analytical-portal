from common.ftp_helper import FTPHelper
from model.file import File
from database_model.db_handler import insert_files

# from ecmwfapi import ECMWFDataServer
#
# server = ECMWFDataServer()
# server.retrieve({
#     "class": "mc",
#     "dataset": "cams_nrealtime",
#     "date": "2017-10-01/to/2017-10-25",
#     "expver": "0001",
#     "levtype": "sfc",
#     "param": "167.128",
#     "step": "0",
#     "stream": "oper",
#     "time": "00:00:00",
#     "type": "an",
#     "target": "output",
# })
#
# print(server)


def main():
    path = '/pub/CDC/observations_germany/climate/'
    list_of_files = FTPHelper().fetch_all_meta_information(path=path)
    insert_files(list_of_files)

main()

# data = [(1,'x'), (2,'y')]
# insert_query = 'insert into t (a, b) values %s'
# psycopg2.extras.execute_values (
#     cursor, insert_query, data, template=None, page_size=100
# )