from logging import exception
from sqlalchemy import null
from Util.db_connection import Db_Connection
from Util.loadproperties import sor_config,stg_config
from datetime import datetime
import pandas as pd
import traceback

def load_channels(revisonId):
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
        #Dictionary for values of channels_tra
        channeldic_tran = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        #Dictionary for values of channels_dim
        channeldic_sor = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        channel_tran = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tran where PROCESOETL_ID={revisonId} ", ses_db_stg)
        channel_sor = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_dim  ", ses_db_sor)
        if not channel_tran.empty:
            for id,des,cla,cla_id \
                in zip(channel_tran['CHANNEL_ID'],channel_tran['CHANNEL_DESC'],
                channel_tran['CHANNEL_CLASS'], channel_tran['CHANNEL_CLASS_ID']
                ):
                channeldic_tran["channel_id"].append(id)
                channeldic_tran["channel_desc"].append(des)
                channeldic_tran["channel_class"].append(cla)
                channeldic_tran["channel_class_id"].append(cla_id)

        if not channel_sor.empty:
            for id,des,cla,cla_id \
                in zip(channel_sor['CHANNEL_ID'],channel_sor['CHANNEL_DESC'],
                channel_sor['CHANNEL_CLASS'], channel_sor['CHANNEL_CLASS_ID']
                ):
                channeldic_sor["channel_id"].append(id)
                channeldic_sor["channel_desc"].append(des)
                channeldic_sor["channel_class"].append(cla)
                channeldic_sor["channel_class_id"].append(cla_id)
        if channeldic_sor["channel_id"]:
            df_channels_tra = pd.DataFrame(channeldic_tran)
            df_channels_sor = pd.DataFrame(channeldic_sor)
            fusion = df_channels_tra.merge(df_channels_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('channels_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_channels_tra = pd.DataFrame(channeldic_tran)
            df_channels_tra.to_sql('channels_dim', ses_db_sor, if_exists="append",index=False)
            



    except:
        traceback.print_exc()
    finally:
        pass