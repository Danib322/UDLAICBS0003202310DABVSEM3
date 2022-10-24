from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config,sor_config,route
from datetime import date, datetime
import pandas as pd
import traceback

from trasnform.FuncionesTransformacion import convertFecha

def load_times(revisonId):
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
        timesdict_tran = {
            "time_id":[],
            "day_name":[],
            "day_integer_in_week":[],
            "day_integer_in_month":[],
            "calendar_week_integer":[],
            "calendar_month_integer":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }
        timesdict_sor = {
            "time_id":[],
            "day_name":[],
            "day_integer_in_week":[],
            "day_integer_in_month":[],
            "calendar_week_integer":[],
            "calendar_month_integer":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }
        times_tran = pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times where PROCESOETL_ID={revisonId}", ses_db_stg)
        times_sor = pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_dim", ses_db_sor)
        #Process
        if not times_tran.empty:
            for id,dname,dinweek,dinmo,caweek,camoin,camode,encamo,caqude,caye \
                in zip(times_tran['TIME_ID'],times_tran['DAY_NAME'],
                times_tran['DAY_INTEGER_IN_WEEK'], times_tran['DAY_INTEGER_IN_MONTH'],
                times_tran['CALENDAR_WEEK_INTEGER'],times_tran['CALENDAR_MONTH_INTEGER'],times_tran['CALENDAR_MONTH_DESC'],
                times_tran['END_OF_CAL_MONTH'],times_tran['CALENDAR_QUARTER_DESC'],times_tran['CALENDAR_YEAR']):
                timesdict_tran["time_id"].append(id)
                timesdict_tran["day_name"].append(dname)
                timesdict_tran["day_integer_in_week"].append(dinweek)
                timesdict_tran["day_integer_in_month"].append(dinmo)
                timesdict_tran["calendar_week_integer"].append(caweek)
                timesdict_tran["calendar_month_integer"].append(camoin)
                timesdict_tran["calendar_month_desc"].append(camode)
                timesdict_tran["end_of_cal_month"].append(encamo)
                timesdict_tran["calendar_month_name"].append(" ")
                timesdict_tran["calendar_quarter_desc"].append(caqude)
                timesdict_tran["calendar_year"].append(caye)
        if not times_sor.empty:
            for id,dname,dinweek,dinmo,caweek,camoin,camode,encamo,caqude,caye \
                in zip(times_sor['TIME_ID'],times_sor['DAY_NAME'],
                times_sor['DAY_INTEGER_IN_WEEK'], times_sor['DAY_INTEGER_IN_MONTH'],
                times_sor['CALENDAR_WEEK_INTEGER'],times_sor['CALENDAR_MONTH_INTEGER'],times_sor['CALENDAR_MONTH_DESC'],
                times_sor['END_OF_CAL_MONTH'],times_sor['CALENDAR_QUARTER_DESC'],times_sor['CALENDAR_YEAR']):
                timesdict_sor["time_id"].append(id)
                timesdict_sor["day_name"].append(dname)
                timesdict_sor["day_integer_in_week"].append(dinweek)
                timesdict_sor["day_integer_in_month"].append(dinmo)
                timesdict_sor["calendar_week_integer"].append(caweek)
                timesdict_sor["calendar_month_integer"].append(camoin)
                timesdict_sor["calendar_month_desc"].append(camode)
                timesdict_sor["end_of_cal_month"].append(encamo)
                timesdict_sor["calendar_month_name"].append(" ")
                timesdict_sor["calendar_quarter_desc"].append(caqude)
                timesdict_sor["calendar_year"].append(caye)
    
        if timesdict_sor["time_id"]:
            df_times_tra = pd.DataFrame(timesdict_tran)
            df_times_sor = pd.DataFrame(timesdict_sor)
            fusion = df_times_tra.merge(df_times_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('times_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_times_tra = pd.DataFrame(timesdict_tran)
            df_times_tra.to_sql('times_dim', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass