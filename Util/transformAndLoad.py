from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config
from trasnform.tran_products import tran_products
from trasnform.tran_promotions import tran_promotions
from trasnform.tran_sales import tran_sales
from trasnform.tran_times import tran_times
from trasnform.tran_channels import tran_channels
from trasnform.tran_countries import tran_countries
from trasnform.tran_customers import tran_customers
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
        #tran_channels(revisonId)
        #tran_countries(revisonId)
        #tran_times(revisonId)
        #tran_customers(revisonId)
        #tran_products(revisonId)
        #tran_promotions(revisonId)
        tran_sales(revisonId)