from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config
from trasnform.tran_channels import tran_channels
from trasnform.tran_countries import tran_countries
from Util.revisionETL import etl_version

def transform():
        stgconfig=stg_config()
        type = stgconfig['TYPE']
        host = stgconfig['HOST']
        port = stgconfig['PORT']
        user = stgconfig['USER']
        pwd = stgconfig['PASSWORD']
        db = stgconfig['DATABASE']
        
        con_db_stg = Db_Connection(type,host,port,user,pwd,db)
        ses_db_stg = con_db_stg.start()
        ses_db_stg.connect()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying connect to the database")
        revisonId=etl_version()
        tran_channels(revisonId)
        tran_countries(revisonId)