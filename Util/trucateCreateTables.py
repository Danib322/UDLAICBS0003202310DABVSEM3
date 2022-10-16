from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config
from  extract.ext_channels import  ext_channels
from  extract.ext_countries import  ext_countries
from  extract.ext_customers import  ext_customers
from  extract.ext_products import  ext_products
from  extract.ext_promotion import  ext_promotions
from  extract.ext_times import  ext_times
from  extract.ext_sales import  ext_sales

def truncarTablas():
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
        ses_db_stg.execute("SET FOREIGN_KEY_CHECKS = 0")
        ses_db_stg.execute("TRUNCATE TABLE channels_ext")
        ses_db_stg.execute("TRUNCATE TABLE countries_ext")
        ses_db_stg.execute("TRUNCATE TABLE customers_ext")
        ses_db_stg.execute("TRUNCATE TABLE products_ext")
        ses_db_stg.execute("TRUNCATE TABLE promotions_ext")
        ses_db_stg.execute("TRUNCATE TABLE sales_ext")
        ses_db_stg.execute("TRUNCATE TABLE times_ext")
        ses_db_stg.execute("SET FOREIGN_KEY_CHECKS = 1")
        ses_db_stg.dispose()

def cargarTablas():
    ext_times()
    ext_channels()
    ext_countries()
    ext_promotions()
    ext_customers()
    ext_products()
    ext_sales()
    
    
