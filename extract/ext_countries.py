from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config,sor_config,route
from datetime import datetime
import pandas as pd
import traceback

def ext_countries():
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
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }
        countries_csv = pd.read_csv(f'{ruta}countries.csv')
        #Process CSV Content
        if not countries_csv.empty:
            for id,nam,reg,reg_id \
                in zip(countries_csv['COUNTRY_ID'],countries_csv['COUNTRY_NAME'],
                countries_csv['COUNTRY_REGION'], countries_csv['COUNTRY_REGION_ID']):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(nam)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(reg_id)
        if countries_dict["country_id"]:
            ses_db_stg.connect().execute("DELETE FROM countries_ext")
            df_countries_ext = pd.DataFrame(countries_dict)
            df_countries_ext.to_sql('countries_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass