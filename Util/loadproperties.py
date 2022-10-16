from jproperties import Properties
"""Stg properties"""


def stg_config():
    configs = Properties()
    with open('properties\stg.properties', 'rb') as stg_config_file:
        configs.load(stg_config_file)
    stg_config={
        "TYPE": configs.get("TYPE").data,
        "HOST":configs.get("DB_HOST").data,
        'PORT':configs.get("DB_PORT").data,
        'USER':configs.get("DB_USER").data,
        'PASSWORD':configs.get("DB_PWD").data,
        'DATABASE':configs.get("DB_SCHEMA").data  
    }
    return stg_config


"""Sor properties"""

configs = Properties()
with open('properties\sor.properties', 'rb') as sor_config_file:
    configs.load(sor_config_file)

def sor_config():
    sor_config={
        'TYPE': configs.get("TYPE").data,
        'HOST':configs.get("DB_HOST").data,
        'PORT':configs.get("DB_PORT").data,
        'USER':configs.get("DB_USER").data,
        'PASSWORD':configs.get("DB_PWD").data,
        'DATABASE':configs.get("DB_SCHEMA").data  
    }
    return sor_config


"""datos properties"""
configs = Properties()
with open('properties\datos.properties', 'rb') as data_file:
    configs.load(data_file)

def route():
        data_route=configs.get("PATH").data
        return data_route

