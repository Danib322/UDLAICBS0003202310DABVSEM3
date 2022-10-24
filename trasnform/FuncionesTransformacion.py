from datetime import datetime
from logging import exception
from Util.db_connection import Db_Connection
from Util.loadproperties import sor_config
import pandas as pd

def obtener_mes(mes):
    return datetime.strptime(str(mes),'%m').strftime('%B').upper()
def parcedate (date):
    return datetime.strptime(date,'%Y-%m-d%')

def convertFecha(datevar):
    fecha_str = datevar
    fecha =  datetime.strptime(fecha_str,'%d-%b-%y')
    return (fecha)

def mapeoCountries(countrie_id, countriesor):
        
        arreglo=dict()
        if not countriesor.empty:
            for id,cou_id \
                in zip(countriesor['ID'],countriesor['COUNTRY_ID']):
                arreglo[cou_id] = id
        return arreglo[countrie_id]

