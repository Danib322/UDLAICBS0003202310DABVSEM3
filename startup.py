import traceback
from Util.transformAndLoad import transform
from  Util.trucateCreateTables import truncarTablas
from  Util.trucateCreateTables import cargarTablas

try:

    #Funciones que truncan y cargan todas las tablas
    
    #truncarTablas()
    #cargarTablas()
    transform()
    
    

except:

    traceback.print_exc()

finally:

    pass