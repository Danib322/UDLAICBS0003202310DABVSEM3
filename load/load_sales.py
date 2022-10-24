from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import stg_config,sor_config,route
from datetime import datetime
import pandas as pd
import traceback


def load_sales(revisonId):
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
        salesdict_tran = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
           
        }
        salesdict_sor = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
           
        }
        sales_tran = pd.read_sql(f"SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD  FROM sales_tran where PROCESOETL_ID={revisonId}", ses_db_stg)
        sales_sor = pd.read_sql(f"SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD  FROM sales", ses_db_sor)
        product_sor=pd.read_sql(f"SELECT ID,PROD_ID FROM products_dim", ses_db_sor)
        customer_sor=pd.read_sql(f"SELECT ID,CUST_ID FROM customers_dim", ses_db_sor)
        time_sor=pd.read_sql(f"SELECT ID,TIME_ID FROM times_dim", ses_db_sor)
        channel_sor=pd.read_sql(f"SELECT ID,CHANNEL_ID FROM channels_dim", ses_db_sor)
        promo_sor=pd.read_sql(f"SELECT ID,PROMO_ID FROM promotions_dim", ses_db_sor)

        dictProdcut=dict()
        if not product_sor.empty:
            for id, proid\
                in zip(product_sor['ID'],product_sor['PROD_ID']):
                dictProdcut[proid] = id

        dictCustomers=dict()
        if not customer_sor.empty:
            for id, cusid\
                in zip(customer_sor['ID'],customer_sor['CUST_ID']):
                dictCustomers[cusid] = id

        dictTimes=dict()
        if not time_sor.empty:
            for id, timid\
                in zip(time_sor['ID'],time_sor['TIME_ID']):
                dictTimes[timid] = id

        dictChannels=dict()
        if not channel_sor.empty:
            for id, chid\
                in zip(channel_sor['ID'],channel_sor['CHANNEL_ID']):
                dictChannels[chid] = id

        dictPromos=dict()
        if not promo_sor.empty:
            for id, proid\
                in zip(promo_sor['ID'],promo_sor['PROMO_ID']):
                dictPromos[proid] = id


        #Process 
        if not sales_tran.empty:
            for id,cuid,tiid,chid,proid,quso,amso \
                in zip(sales_tran['PROD_ID'],sales_tran['CUST_ID'],
                sales_tran['TIME_ID'], sales_tran['CHANNEL_ID'],
                sales_tran['PROMO_ID'],sales_tran['QUANTITY_SOLD'],sales_tran['AMOUNT_SOLD']):
                salesdict_tran["prod_id"].append(dictProdcut[id])
                salesdict_tran["cust_id"].append(dictCustomers[cuid])
                salesdict_tran["time_id"].append(dictTimes[tiid])
                salesdict_tran["channel_id"].append(dictChannels[chid])
                salesdict_tran["promo_id"].append(dictPromos[proid])
                salesdict_tran["quantity_sold"].append(quso)
                salesdict_tran["amount_sold"].append(amso)
        if not sales_sor.empty:
            for id,cuid,tiid,chid,proid,quso,amso \
                in zip(sales_sor['PROD_ID'],sales_sor['CUST_ID'],
                sales_sor['TIME_ID'], sales_sor['CHANNEL_ID'],
                sales_sor['PROMO_ID'],sales_sor['QUANTITY_SOLD'],sales_sor['AMOUNT_SOLD']):
                salesdict_sor["prod_id"].append(id)
                salesdict_sor["cust_id"].append(cuid)
                salesdict_sor["time_id"].append(tiid)
                salesdict_sor["channel_id"].append(chid)
                salesdict_sor["promo_id"].append(proid)
                salesdict_sor["quantity_sold"].append(quso)
                salesdict_sor["amount_sold"].append(amso)
        if salesdict_sor["prod_id"]:
            df_sales_tra = pd.DataFrame(salesdict_tran)
            df_sales_sor = pd.DataFrame(salesdict_sor)
            fusion = df_sales_tra.merge(df_sales_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('sales', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_sales_tra = pd.DataFrame(salesdict_tran)
            df_sales_tra.to_sql('sales', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass