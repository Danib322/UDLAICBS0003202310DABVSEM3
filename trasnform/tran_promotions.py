from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config,sor_config,route
from datetime import datetime
import pandas as pd
import traceback

from trasnform.FuncionesTransformacion import convertFecha

def tran_promotions(revisonId):
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
            "promo_end_date":[],
            "procesoetl_id":[]
        }
        promotions_ext = pd.read_sql("SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_ext", ses_db_stg)
        #Process CSV Content
        if not promotions_ext.empty:
            for id,pna,pco,pbeda,penda \
                in zip(promotions_ext['PROMO_ID'],promotions_ext['PROMO_NAME'],
                promotions_ext['PROMO_COST'], promotions_ext['PROMO_BEGIN_DATE'],
                promotions_ext['PROMO_END_DATE']):
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(pna)
                promotions_dict["promo_cost"].append(pco)
                promotions_dict["promo_begin_date"].append(convertFecha(pbeda))
                promotions_dict["promo_end_date"].append(convertFecha(penda))
                promotions_dict["procesoetl_id"].append(revisonId)
        if promotions_dict["promo_id"]:
            df_promotions_tran = pd.DataFrame(promotions_dict)
            df_promotions_tran.to_sql('promotions_tran', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass