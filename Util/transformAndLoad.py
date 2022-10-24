from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config
from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times
from trasnform.tran_products import tran_products
from trasnform.tran_promotions import tran_promotions
from trasnform.tran_sales import tran_sales
from trasnform.tran_times import tran_times
from trasnform.tran_channels import tran_channels
from trasnform.tran_countries import tran_countries
from trasnform.tran_customers import tran_customers
from Util.revisionETL import etl_version

revisonId=etl_version()
def transform():
        
        tran_channels(revisonId)
        tran_countries(revisonId)
        tran_times(revisonId)
        tran_customers(revisonId)
        tran_products(revisonId)
        tran_promotions(revisonId)
        tran_sales(revisonId)
    
def CragarSor():
    load_channels(revisonId)
    load_countries(revisonId)
    load_customers(revisonId)
    load_products(revisonId)
    load_promotions(revisonId)
    load_sales(revisonId)
    load_times(revisonId)