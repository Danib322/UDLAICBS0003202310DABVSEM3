import traceback
from  Util.trucateCreateTables import truncarTablas
from  Util.trucateCreateTables import cargarTablas
from  extract.ext_channels import  ext_channels
from  extract.ext_countries import  ext_countries
from  extract.ext_customers import  ext_customers
from  extract.ext_products import  ext_products
from  extract.ext_promotion import  ext_promotions
from  extract.ext_times import  ext_times
from  extract.ext_sales import  ext_sales

try:

    #Funciones que truncan y cargan todas las tablas
    truncarTablas()
    cargarTablas()
    #Para probar tablas de manera unitaria descomentar la tabla deseada y comentar las funciones
    #ext_times()
    #ext_channels()
    #ext_countries()
    #ext_promotions()
    #ext_customers()
    #ext_products()
    #ext_sales()

except:

    traceback.print_exc()

finally:

    pass