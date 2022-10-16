from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config,sor_config,route
from datetime import datetime
import pandas as pd
import traceback

def ext_sales():
    try:
        #Variable que trae la funcion de properties
        stgconfig=stg_config()
        ruta=route()
        type = stgconfig['TYPE']
        host = stgconfig['HOST']
        port = stgconfig['PORT']
        user = stgconfig['USER']
        pwd = stgconfig['PASSWORD']
        db = stgconfig['DATABASE']
        
        con_db_stg = Db_Connection(type,host,port,user,pwd,db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying connect to the database")
        #Dictionary for values 
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }
        sales_csv = pd.read_csv(f'{ruta}sales.csv')
        #Process CSV Content
        if not sales_csv.empty:
            for id,cuid,tiid,chid,proid,quso,amso \
                in zip(sales_csv['PROD_ID'],sales_csv['CUST_ID'],
                sales_csv['TIME_ID'], sales_csv['CHANNEL_ID'],
                sales_csv['PROMO_ID'],sales_csv['QUANTITY_SOLD'],sales_csv['AMOUNT_SOLD']):
                sales_dict["prod_id"].append(id)
                sales_dict["cust_id"].append(cuid)
                sales_dict["time_id"].append(tiid)
                sales_dict["channel_id"].append(chid)
                sales_dict["promo_id"].append(proid)
                sales_dict["quantity_sold"].append(quso)
                sales_dict["amount_sold"].append(amso)
        if sales_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE sales_ext")
            df_sales_ext = pd.DataFrame(sales_dict)
            df_sales_ext.to_sql('sales_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass