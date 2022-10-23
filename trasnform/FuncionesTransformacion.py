from datetime import datetime

def obtener_mes(mes):
    return datetime.strptime(str(mes),'%m').strftime('%B').upper()
def parcedate (date):
    return datetime.strptime(date,'%Y-%m-d%')