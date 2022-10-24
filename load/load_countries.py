from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config,sor_config,route
from datetime import datetime
import pandas as pd
import traceback

def load_countries(revisonId):
    try:
        #conexion con stg
        stgconfig=stg_config()
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
        #conexion con sor 
        sorconfig= sor_config()
        type = sorconfig['TYPE']
        host = sorconfig['HOST']
        port = sorconfig['PORT']
        user = sorconfig['USER']
        pwd = sorconfig['PASSWORD']
        db = sorconfig['DATABASE']
        
        con_db_sor = Db_Connection(type,host,port,user,pwd,db)
        ses_db_sor = con_db_sor.start()
        if ses_db_sor == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_sor == -2:
            raise Exception("Error trying connect to the database")
        #Dictionary for values
        countriesdict_tran = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            
            
        }
        countriesdict_sor = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            
            
        }
        countries_tran = pd.read_sql(f"SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_tran where PROCESOETL_ID={revisonId}", ses_db_stg)
        countries_sor = pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_dim", ses_db_sor)
        #Process CSV Content
        if not countries_tran.empty:
            for id,nam,reg,reg_id \
                in zip(countries_tran['COUNTRY_ID'],countries_tran['COUNTRY_NAME'],
                countries_tran['COUNTRY_REGION'], countries_tran['COUNTRY_REGION_ID']):
                countriesdict_tran["country_id"].append(id)
                countriesdict_tran["country_name"].append(nam)
                countriesdict_tran["country_region"].append(reg)
                countriesdict_tran["country_region_id"].append(reg_id)
        if not countries_sor.empty:
            for id,nam,reg,reg_id \
                in zip(countries_sor['COUNTRY_ID'],countries_sor['COUNTRY_NAME'],
                countries_sor['COUNTRY_REGION'], countries_sor['COUNTRY_REGION_ID']):
                countriesdict_sor["country_id"].append(id)
                countriesdict_sor["country_name"].append(nam)
                countriesdict_sor["country_region"].append(reg)
                countriesdict_sor["country_region_id"].append(reg_id)

        if countriesdict_sor["country_id"]:
            df_countries_tra = pd.DataFrame(countriesdict_tran)
            df_countries_sor = pd.DataFrame(countriesdict_sor)
            fusion = df_countries_tra.merge(df_countries_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('countries_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_channels_tra = pd.DataFrame(countriesdict_tran)
            df_channels_tra.to_sql('countries_dim', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass