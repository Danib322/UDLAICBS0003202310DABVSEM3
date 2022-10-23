from datetime import datetime

def obtener_mes(mes):
    return datetime.strptime(str(mes),'%m').strftime('%B').upper()
def parcedate (date):
    return datetime.strptime(date,'%Y-%m-d%')

def convertFecha(datevar):
    fecha_str = datevar
    fecha =  datetime.strptime(fecha_str,'%d-%b-%y')
    return (fecha)