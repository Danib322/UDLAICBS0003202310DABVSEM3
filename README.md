# UDLAICBS0003202310DABVSEM3
Nombre: Daniel Alexander Bustos Velez
<h1>Titulo</h1>
<p>Implementación Bodegas de Datos</p>
<h1>Definición del Proyecto</h1>
<p>Con el SGDB MySQL Workbrench y MySQL 8.0 crear dos modelos de base de datos (Staging , SORE) que servirán para la implementación de un DHW</p>
<h1>Indicaciones para su uso</h1>
<p>1.Clonar el repositorio con el comando "git -clone" en la carpeta de destino</p>
<p>2.Abrir la carpeta con el IDE de preferencia o desde terminal recomiendo VS Code</p>
<p>3.En MySQL ejecutar los archivos que se encuentran en la carpeta "scripts"</p>
<p>4. En caso de no tener instalada una libreria que requiera el proyecto puede hacerlo desde terminal con el comando pip install "librería" </p>
<p>5.Todo listo para ejecutar el proyecto!! </p>
<h1>Librerias Necesarias para ejecutar el Proyecto</h1>
<p>Loggin</p>
<p>Pandas</p>
<p>Traceback</p>
<p>sqlalchemy</p>
<p>DateTime</p>
<p>jproperties</p>
<h1>Consideraciones Importantes</h1>
<p>La carpeta properties contiene los archivos ".properties" que son necesarios para el correcto funcionamineot del programa. Al momento de ejecutar en su instancia local caragar los datos de su instancia de MySQL</p>
<p>En la carpeta "Util" se encuentra tres archivos el primero permite la conexión con la base de datos</p>
<p>El segundo carga las los archivos properties y los formatea para utilizarlos en los archivos extrac</p>
<p>El Tercero se encuentran las funciones que permiten truncar todas las tablas y volver a cargarlos, estas funciones se implementan desde el archivo "startup"</p>
<p>Si se desea ejecutar de manera unitaria las tablas de extración, descomentar la tabla que se de desea probar del archivo "startup" y comentar las funciones "truncarTablas()" y "cargarTablas()"con "#"</p>