cxsafrom airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.utils.trigger_rule import TriggerRule
from jsonschema import draft4_format_checker
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from tabula import read_pdf 
from tabulate import tabulate
from datetime import datetime 

from pyhive import hive
 
from sqlalchemy import create_engine
 
from urllib.parse import urlparse

import functools
import random

import pandas
import os
import requests
import pydoop.hdfs as hd

tmpdir="/home/hadoop/tmpfinal/"
tmpcsv="/home/hadoop/tmpcsv/"

def cargar_archivos():  

    #Cadena de conexion base de datos  
    cnn=MySqlHook("miConectorEvaluacionFinal").get_sqlalchemy_engine()

    #Cargamos el dataframe pedidos
    df_ventas_pedidos = pandas.read_sql("SELECT * FROM Pedidos", cnn)
    #df_ventas_pedidos.to_csv(tmpcsv+"pedidos.csv", sep=';', encoding='utf-8',index=False)

    #Cargamos el dataframe de detalle
    df_ventas_detalle = pandas.read_sql("SELECT * FROM PedidosDetalles", cnn)    
    df_ventas_detalle.rename(columns={'CodSubcategoria':'CodSubCategoria'},inplace=True)
    #df_ventas_detalle.to_csv(tmpcsv+"detalles.csv", sep=';',encoding='utf-8',index=False)

    # Cargamos el dataframe paises
    df_paises_aux = read_pdf(tmpdir+'PaisTabulado.pdf', 
        pages = ["all"],
        multiple_tables = True,
        pandas_options={'header':None},
        encoding = 'ISO-8859-1', 
        stream = False
    )
    df_paises_aux=df_paises_aux[0]
    df_para_concat=[df_paises_aux]
    df_paises=pandas.concat(df_para_concat)

    # primera fila como encabezado y copiamos en archivo temporal
    df_paises.columns=df_paises.iloc[0]
    df_paises=df_paises[1:]
    #df_paises.to_csv(tmpcsv+"paises.csv", sep=';', encoding='utf-8',index=False)
    
      
    # Cargamos el dataframe clientes
    df_clientes = pandas.read_excel(tmpdir+'Maestros.xlsx', sheet_name='Clientes')
    df_clientes.rename(columns={'NroCliente':'IDCliente'},inplace=True)
    #df_clientes.to_csv(tmpcsv+"clientes.csv", sep=';', encoding='utf-8',index=False)
   
    # Cargamos el dataframe productos y copiamos en archivo temporal
    df_productos_aux = pandas.read_excel(tmpdir+'Maestros.xlsx', sheet_name='Productos')     
    df_productos=df_productos_aux.drop_duplicates('NroProducto', keep='last')
    df_productos.rename(columns={'NroProducto':'IDProducto'},inplace=True)
    #df_productos.to_csv(tmpcsv+"productos.csv", sep=';', encoding='utf-8',index=False)


    # Cargamos el dataframe productos y copiamos en archivo temporal   
    df_ciudades = pandas.read_csv(tmpdir+'Ciudades.csv',sep=';',decimal=',')    
    df_ciudades.rename(columns={'Pais':'CodigoPais', 'CODCiudad':'CiudadDespacho' },inplace=True)
    #df_ciudades.to_csv(tmpcsv+"ciudades.csv", sep=';', encoding='utf-8',index=False)

    # Cargamos el dataframe categorias y copiamos en archivo temporal
    df_categorias = pandas.read_excel(tmpdir+'TablasAuxiliares.xlsx', usecols=["CODCategoria","Categoria"])
    df_categorias.dropna(inplace=True)
    #df_categorias.to_csv(tmpcsv+"categorias.csv", sep=';', encoding='utf-8',index=False)
    
    # Cargamos el dataframe subcategoria y copiamos en archivo temporal
    df_subcategorias = pandas.read_excel(tmpdir+'TablasAuxiliares.xlsx', usecols=["CODCategoria","CODSubCategoria", "SubCategoria"])
    df_subcategorias.dropna(inplace=True)
    df_subcategorias.rename(columns={'CODSubCategoria':'CodSubCategoria'},inplace=True)
    #df_subcategorias.to_csv(tmpcsv+"subcategorias.csv", sep=';', encoding='utf-8',index=False)
   

    # Cargamos el dataframe segmento y copiamos en archivo temporal
    df_segmentos = pandas.read_excel(tmpdir+'TablasAuxiliares.xlsx', usecols=["CODSegmento","Segmento"])
    df_segmentos.rename(columns={'CODSegmento':'IDSegmento'},inplace=True)
    df_segmentos.dropna(inplace=True)
    #df_segmentos.to_csv(tmpcsv+"segmentos.csv", sep=';', encoding='utf-8',index=False)

    # Cargamos el dataframe modoenvio y copiamos en archivo temporal
    df_modoenvios = pandas.read_excel(tmpdir+'TablasAuxiliares.xlsx', usecols=["CODModoEnvio","Modo_Envío"])
    df_modoenvios.dropna(inplace=True)
    df_modoenvios.rename(columns={'CODModoEnvio':'IDModoEnvio'},inplace=True)
    #df_modoenvios.to_csv(tmpcsv+"modoenvios.csv", sep=';', encoding='utf-8',index=False)


    # JOIN 
    # de los datafrmae pedido y detalle
    #df_join_pedidos_detalles=df_ventas_pedidos join(df_ventas_detalle.set_index('NroPedido'), on='NroPedido',how='left') 
    df_join_pedidos_detalles= pandas.merge(df_ventas_pedidos, df_ventas_detalle, on='NroPedido', how='right')
    #df_join_pedidos_detalles.to_csv(tmpcsv+"joinpedidosdetalles.csv", sep=';', encoding='utf-8',index=False)
    #df_join_pedidos_detalles.to_csv(tmpcsv+"joinpedidosdetalles.csv", sep=';', encoding='utf-8',index=False)

    # de ciudades y paises
    df_join_ciudades_paises=df_ciudades.join(df_paises.set_index('CodigoPais'), on='CodigoPais')
    #df_join_ciudades_paises.to_csv(tmpcsv+"joinciudadespaises.csv", sep=';', encoding='utf-8',index=False)
    #df_join_pedidos_detalles.to_csv(tmpcsv+"joinciudadespaises.csv", sep=';', encoding='utf-8',index=False)

    # de categorias y subcategoria
    df_join_categorias_subcategorias=df_subcategorias.join(df_categorias.set_index('CODCategoria'), on='CODCategoria')
    #df_join_categorias_subcategorias.to_csv(tmpcsv+"joincategoriassubcategorias.csv", sep=';', encoding='utf-8',index=False)

    # de clientes y segmentos
    df_join_clientes_segmentos=df_clientes.join(df_segmentos.set_index('IDSegmento'), on='IDSegmento')
    #df_join_clientes_segmentos.to_csv(tmpcsv+"joinclientessegementos.csv", sep=';', encoding='utf-8',index=False)

    df_join_pedido_detalle_clientes=df_join_pedidos_detalles.join(df_join_clientes_segmentos.set_index('IDCliente'), on='IDCliente')
    df_join_pedido_detalle_clientes_productos=df_join_pedido_detalle_clientes.join(df_productos.set_index('IDProducto'), on='IDProducto')
    df_join_pedido_detalle_clientes_productos_ciudad=df_join_pedido_detalle_clientes_productos.join(df_join_ciudades_paises.set_index('CiudadDespacho'), on='CiudadDespacho')
    df_join_pedido_detalle_clientes_productos_ciudad_envio=df_join_pedido_detalle_clientes_productos_ciudad.join(df_modoenvios.set_index('IDModoEnvio'), on='IDModoEnvio')
    #df_join_pedido_detalle_clientes_productos_ciudad_envio_subcategoria=df_join_pedido_detalle_clientes_productos_ciudad_envio.join(df_join_categorias_subcategorias.set_index('CodSubCategoria'), on='CodSubcategoria')
    
    df_final=df_join_pedido_detalle_clientes_productos_ciudad_envio
    df_final['Cantidad'] = df_final['Cantidad'].astype(float)
    df_final['PrecioUnitario'] = df_final['PrecioUnitario'].astype(float)
    df_final['Descuento'] = df_final['Descuento'].astype(float)


    df_final["TotalVenta_USD"]= df_final["Cantidad"]*df_final["PrecioUnitario"]-df_final["Cantidad"]*df_final["PrecioUnitario"]*df_final["Descuento"]
    df_final["TotalVenta_CLP"]= df_final["TotalVenta_USD"]*valor_dolar()



    df_final.to_csv(tmpcsv+"joinfinalconnombrecampo.csv", sep=';', encoding='utf-8',index=False)

    df_final.to_csv(tmpcsv+"joinfinalsinnombrecampo.csv", sep=';', encoding='utf-8',index=False, header=None)



    return

# funcion valor dolar 
def valor_dolar():
    # configuracion de la cadena de conexion a la URL del banco central
    url_dolar="https://si3.bcentral.cl/indicadoressiete/secure/Serie.aspx?gcode=PRE_TCO&param=RABmAFYAWQB3AGYAaQBuAEkALQAzADUAbgBNAGgAaAAkADUAVwBQAC4AbQBYADAARwBOAGUAYwBjACMAQQBaAHAARgBhAGcAUABTAGUAdwA1ADQAMQA0AE0AawBLAF8AdQBDACQASABzAG0AXwA2AHQAawBvAFcAZwBKAEwAegBzAF8AbgBMAHIAYgBDAC4ARQA3AFUAVwB4AFIAWQBhAEEAOABkAHkAZwAxAEEARAA="
    page=requests.get(url_dolar)
    soup=BeautifulSoup(page.text,'lxml')

    df_tabla_dolar=soup.find("table", attrs={"id":"gr", "class":"table"})

    # extraacion de encabezados de las tabla
    encabezados=[]
    for i in df_tabla_dolar.find_all('th'):
        aux=i.text
        encabezados.append(aux)

    encabezados2=encabezados
    for j in range(1,len(encabezados)):
        encabezados2[j]=str(j).zfill(2)

    df_dolar=pandas.DataFrame(columns=encabezados2)
    for k in df_tabla_dolar.find_all('tr')[1:]:
        valor_celda=k.find_all('td')
        fila=[tr.text for tr in valor_celda]
        df_dolar.loc[len(df_dolar)]=fila

    # Permite despivotear la tabla de valor dolar
    df_dolar_unpivoted = df_dolar.melt(id_vars=['Día'], var_name='Mes', value_name='Valor')
   
    # Convertimos el formato de fechas
    df_dolar_unpivoted["Fecha"]="2022"+"-"+ df_dolar_unpivoted["Mes"]+"-"+df_dolar_unpivoted["Día"].apply(lambda x: '{0:0>2}'.format(x))

    # cambiar los valores nulo por NaN
    nan_value=float("NaN")
    df_dolar_unpivoted.replace("",nan_value, inplace=True)  
    # Eliminar los lineas de las tablas con valor NaN
    df_dolar_unpivoted.dropna(subset=["Valor"], inplace=True)
    # Eliminar las columnas Dia y >Mes
    df_dolar_unpivoted.drop(['Día','Mes'], axis=1, inplace=True)

    # ordernamos la tablas ascendentemente
    df_dolar_unpivoted.sort_values(by=['Fecha'], ascending=False, inplace=  True)
    ValorHoy=df_dolar_unpivoted.iloc[0]
    
    # reemplazamos la coma por punto
    ValorHoy[0]=ValorHoy[0].replace(",",".")
    ValorHoy[0]=float(ValorHoy[0])
    print(ValorHoy[0])

    return (ValorHoy[0])

# funcion lectura archivos hdfs
def hdsf_archivos():
  
    # lectura pedidos
    with hd.open("/evaluacionfinal/joinfinal.csv") as f:        
        df_pedidos = pandas.read_csv(f,sep=';',decimal=',')
        print(df_pedidos)
    
    return

# funcion lectura archivos hdfs
def conn_hive():
    hive_hook = HiveServer2Hook("JIM_Hive").get_uri()
    hive_hook = hive_hook.replace("hiveserver2","hive",1)
    hive_hook = urlparse(hive_hook)
    hive_hook = hive_hook._replace(netloc="{}@{}".format(hive_hook.username, hive_hook.hostname))
    hive_hook = hive_hook.geturl()
    engine = create_engine(hive_hook)
    
    return
##########################################################

def via(options):
    return random.choice(options)

def mySql_Local():
    hive_hook = HiveServer2Hook("JIM_Hive").get_uri()
    hive_hook = hive_hook.replace("hiveserver2","hive",1)
    hive_hook = urlparse(hive_hook)
    hive_hook = hive_hook._replace(netloc="{}@{}".format(hive_hook.username, hive_hook.hostname))
    hive_hook = hive_hook.geturl()
    engine = create_engine(hive_hook)
 
    result_df = pandas.read_sql("SELECT * FROM jimpro",engine)
    mysql_local = MySqlHook("JIM_AIR_local").get_uri()
    nube = create_engine(mysql_local, encoding="utf8")
    result_df.to_sql("grupo3_g5", nube, if_exists='replace',index=False, chunksize=1000)
    return 

def mySql_Nube():
    hive_hook = HiveServer2Hook("JIM_Hive").get_uri()
    hive_hook = hive_hook.replace("hiveserver2","hive",1)
    hive_hook = urlparse(hive_hook)
    hive_hook = hive_hook._replace(netloc="{}@{}".format(hive_hook.username, hive_hook.hostname))
    hive_hook = hive_hook.geturl()
    engine = create_engine(hive_hook)
 
    result_df = pandas.read_sql("SELECT * FROM jimpro",engine)
    mysql_local = MySqlHook("JIM_AIR_local").get_uri()
    nube = create_engine(mysql_local)
    result_df.to_sql("grupo3_g5", nube, if_exists='replace',index=False, chunksize=1000)
    return 

##########################################################
# dag principal
dag_cargaarchivos = DAG(
    dag_id="evaluacionfinalcurso",
    schedule_interval=None,
    start_date=datetime.today(),
    catchup=False        
)

# dag elimina carpeta archivos directorio temporal
tarea01=BashOperator(
    task_id="borrar_carpeta",
    params={'tmpdir':tmpdir},
    bash_command="rm '{{params.tmpdir}}' -rf && hadoop fs -rm -f -R /evaluacionfinal",
    dag=dag_cargaarchivos
)

# dag crear carpeta archivos directorio temporal 
tarea02=BashOperator(
    task_id="crear_carpeta",
    params={'tmpdir':tmpdir},
    bash_command="mkdir -p '{{params.tmpdir}}' && chmod 777 '{{params.tmpdir}}'",
    dag=dag_cargaarchivos
)

# dag bajar archivo ciudades
tarea03=BashOperator(
    task_id="bajar_ciudades",
    params={'tmpdir':tmpdir},
    bash_command="wget https://fileserver.my-vms.com/PROYECTO%20FINAL%20CORFO/Ciudades.csv -P '{{params.tmpdir}}'",
    dag=dag_cargaarchivos
)

# dag bajar archivo maestro
tarea04=BashOperator(
    task_id="bajar_maestros",
    params={'tmpdir':tmpdir},
    bash_command="wget https://fileserver.my-vms.com/PROYECTO%20FINAL%20CORFO/Maestros.xlsx -P '{{params.tmpdir}}'",
    dag=dag_cargaarchivos
)

#dag bajar archivo pais
tarea05=BashOperator(
    task_id="bajar_paises",
    params={'tmpdir':tmpdir},
    bash_command="wget https://fileserver.my-vms.com/PROYECTO%20FINAL%20CORFO/PaisTabulado.pdf -P '{{params.tmpdir}}'",
    dag=dag_cargaarchivos
)

# dag bajar archivo auxiliar
tarea06=BashOperator(
    task_id="bajar_auxiliares",
    params={'tmpdir':tmpdir},
    bash_command="wget https://fileserver.my-vms.com/PROYECTO%20FINAL%20CORFO/TablasAuxiliares.xlsx -P '{{params.tmpdir}}'",
    dag=dag_cargaarchivos
)

# dag cargar  y convierte archivos csv
tarea07=PythonOperator(
    task_id="consolidar_datos",
    python_callable=cargar_archivos,
    dag=dag_cargaarchivos
)

# dag copia archivos csv a hdfs
tarea08=BashOperator(
    task_id="cargar_hdfs",
    bash_command='{{"/home/hadoop/tmpscripts/scripts.sh "}}',
    dag= dag_cargaarchivos
)    

tarea09 = PythonOperator(
    task_id="conexion_hive",
    python_callable=conn_hive,
    dag=dag_cargaarchivos
)
 
tarea10 = HiveOperator(
    task_id = "cargar_hive",
    hive_cli_conn_id = "JIM_Hive",
    hql = (        
        "USE jim;"
        "DROP TABLE jimpro;"
        "CREATE TABLE jimpro("
        "NroPedido string,"               
        "FechaPedido string,"
        "FechaDespacho string,"
        "IDCliente  string,"   
        "IDModoEnvio string,"    
        "CiudadDespacho string,"   
        "Descuento float,"
        "Linea float,"
        "IDProducto string,"   
        "CodSubCategoria string,"   
        "Cantidad  float,"   
        "PrecioUnitario float,"
        "Margen  float,"
        "CostoEnvio float,"
        "Nombre string," 
        "IDSegmento string," 
        "Segmento string," 
        "Producto string," 
        "CodigoPais string," 
        "Ciudad string," 
        "Pais string," 
        "Modo_Envio string," 
        "TotalVenta_USD float,"
        "TotalVenta_CLP float)"
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';"
        "LOAD DATA INPATH '/evaluacionfinal/joinfinalsinnombrecampo.csv' OVERWRITE INTO TABLE jimpro;"
        ),
    dag=dag_cargaarchivos
)

tareas = ['tarea12', 'tarea13']
branching = BranchPythonOperator(
    task_id='Cargar_Mysql',
    #acá está magia de hacer "callable" una función "no callable" con functools.partial
   	python_callable=functools.partial(via, options=tareas)
)

# dag cargar  y mysqk local
db_local = PythonOperator(
    task_id='db_local',
    python_callable=mySql_Local,
    dag=dag_cargaarchivos,
    #trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

# dag cargar  y mysqk nube
db_nube=PythonOperator(
    task_id="db_nube",
    python_callable=mySql_Nube,
    dag=dag_cargaarchivos,
    #trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

# dag envio de mail
tarea14 = EmailOperator(
    task_id='enviar_correo',
    to=['and.gon.f@gmail.com','oscarmonardezsaguas@gmail.com','Juanignao@yahoo.com','Jonathansilva@live.cl'],
#   cc=[''],
    subject='Notificación ELT Airflow Trabajo Final Grupo 3',
    html_content="Procesamiento exitoso desde Airflow!!! -> Fecha: {{ ds }} <br>Integrantes : Oscar Monardez<br>Andrés González<br>Juan Ignao<br>Jonathan Silva<br><br>Logs adjuntos del proceso ",
    files=['/home/hadoop/scheduler.out','/home/hadoop/webserver.out'],
    dag=dag_cargaarchivos
)


tarea01 >> tarea02 >> tarea03 >> tarea07 >> tarea08 >> tarea09 >> tarea10  >> branching
tarea01 >> tarea02 >> tarea04 >> tarea07 >> tarea08 >> tarea09 >> tarea10  >> branching
tarea01 >> tarea02 >> tarea05 >> tarea07 >> tarea08 >> tarea09 >> tarea10  >> branching
tarea01 >> tarea02 >> tarea06 >> tarea07 >> tarea08 >> tarea09 >> tarea10  >> branching

branching >> db_local >> tarea14 
branching >> db_nube >> tarea14 

