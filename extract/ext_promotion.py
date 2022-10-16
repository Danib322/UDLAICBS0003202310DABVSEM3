from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config,sor_config,route
from datetime import datetime
import pandas as pd
import traceback

def ext_promotions():
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
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        promotions_csv = pd.read_csv(f'{ruta}promotions.csv')
        #Process CSV Content
        if not promotions_csv.empty:
            for id,pna,pco,pbeda,penda \
                in zip(promotions_csv['PROMO_ID'],promotions_csv['PROMO_NAME'],
                promotions_csv['PROMO_COST'], promotions_csv['PROMO_BEGIN_DATE'],
                promotions_csv['PROMO_END_DATE']):
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(pna)
                promotions_dict["promo_cost"].append(pco)
                promotions_dict["promo_begin_date"].append(pbeda)
                promotions_dict["promo_end_date"].append(penda)
        if promotions_dict["promo_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
            df_promotions_ext = pd.DataFrame(promotions_dict)
            df_promotions_ext.to_sql('promotions_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass