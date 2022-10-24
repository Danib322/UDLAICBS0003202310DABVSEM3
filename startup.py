import traceback
from Util.transformAndLoad import CragarSor, transform
from  Util.trucateCreateTables import truncarTablas
from  Util.trucateCreateTables import cargarTablas
from trasnform.FuncionesTransformacion import mapeoCountries


try:

    #Funciones que truncan y cargan todas las tablas
    
    #truncarTablas()
    #cargarTablas()
    transform()
    CragarSor()
        
    

except:

    traceback.print_exc()

finally:

    pass