import traceback
from  Util.trucateCreateTables import truncarTablas
from  Util.trucateCreateTables import cargarTablas

try:

    truncarTablas()
    cargarTablas()

except:

    traceback.print_exc()

finally:

    pass