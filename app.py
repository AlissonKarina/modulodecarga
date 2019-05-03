# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify, json
from flask_cors import CORS
from zipfile import ZipFile
from helpers.campos_excel import formato_one, formato_two
import psycopg2 as ps
import pandas as pd
import os

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
APP_ROOT = os.path.dirname(os.path.abspath(__file__))

name_of_pc = ""
ip = ""
total_registros_procesados = 0
total_registros_insertados = 0
total_registros_excluidos = 0
good_files = []
bad_files = []
duplicados = []
isRepollo=""
# formato_excel = {}
status_indiv_file = 'OK'
msg_error_column = 'El formato del excel no contiene la columna'


# conn = ps.connect(host="localhost", port=5432, dbname="tcs_prueba", user="postgres", password="1234")
# cur = conn.cursor()

@app.route('/')
def hello_world():
    return 'Back de Módulo de carga, ready'


@app.route('/upload', methods=['POST']) 
def upload():
    #TARGET = APP_ROOT + "static/"
    target = os.path.join(APP_ROOT, "static")

    #CHEQUEA SI EL ARCHIVO ESTÁ PRESENTE O NO
    if 'file' not in request.files:
        return "Not file found "

    #EXISTE LA RUTA - TARGET ?
    if not os.path.isdir(target):
        os.mkdir(target) #CREA LA CARPETA target COMO TAL
        global name_of_pc, ip #HABILITA EL CAMBIO DE LAS VARIABLES name_of_pc, ip
    
    #TRAE INFORMACION DE LA VISTA request , INFORAMCION DEL EXCEL
    #file = ************************************************
    #tipo_archivo = TIPO DE ARCHIVO .XLSX , .XLX
    #name_of_pc = NOMBRE DE LA PC
    #IP = IP ESTATICA
    #formato = FORMATO ELEGIDO DESDE LA VISTA
    file = request.files['file']
    tipo_archivo = request.form.get('tipo')
    name_of_pc = request.form.get('name')
    ip = "200.48.225.130" #estatica
    formato = request.form.get('formato')
    
    
    #CREA UN ARRAY respuesta
    respuesta = {}

    #GUARDA EL NOMBRE DEL ARCHIVO EN filename, viene de HTML
    filename = file.filename

    #destination = target + filename
    #SE GUARDA EL ARCHIVO EXCEL EN LA CARPETA CREADA DE DIRECCION target
    destination = "/".join([target, filename])

    file.save(destination)

    """ if tipo_archivo == "zip":
        global total_registros_procesados, total_registros_insertados, total_registros_excluidos
        total_registros_procesados = 0
        total_registros_insertados = 0
        total_registros_excluidos = 0
        #ver
        process_zip_file(destination, filename, int(formato))
        global good_files, bad_files, duplicados
        respuesta = {'file': filename, 'good_files': {'lista_detalle': good_files, 'total_registros_procesados': total_registros_procesados, 'total_registros_insertados': total_registros_insertados,
                     'total_registros_excluidos': total_registros_excluidos}, 'bad_files': bad_files}
        os.remove(destination)
        return jsonify(respuesta) """
    if tipo_archivo == "excel":
        #global duplicados
        reg_procesados, reg_insertados, reg_excluidos = process_excel_file(destination, filename, int(formato))
        respuesta = {'filename': filename, 'status': status_indiv_file, 'registros_procesados': reg_procesados, 'registros_insertados': reg_insertados,
                     'registros_excluidos': reg_excluidos, 'registros_duplicados_detalle': duplicados}
        os.remove(destination)
        return jsonify(respuesta)

#CONEXION A LA BD
def connect_database():
    return ps.connect(host="159.65.230.188", port=5432, dbname="tcs2", user="modulo4", password="modulo4")

#
#path_zip_file = DESTINATION
#filename = NOMBRE DEL EXCEL
#formato= TIPO DE FORMATO
def process_zip_file(path_zip_file, filename, formato):
    global total_registros_procesados, total_registros_insertados, total_registros_excluidos, msg_error_column, good_files, bad_files, duplicados
    formato_excel = set_formato_excel(formato) #OBTIENE EL TIPO DE FORMATO

    archivo_zip = ZipFile(path_zip_file, 'r')
    content_of_zip = archivo_zip.infolist() #CONTENIDO DEL ZIP, ES DECIR UNA LISTA DE EXCEL
    good_files = []
    bad_files = []
    duplicados = []
    extension = (".xls",".xlsx")
    for s in content_of_zip:
        duplicados = []
        if s.filename.endswith(extension): #VERIFICA QUE LA EXTENSION DEL ARCHIVO SEA .xls .xlsx
            print(s.filename)
            try:
                df = pd.read_excel(archivo_zip.open(s.filename, 'r'), converters=formato_excel) #Obtiene primer excel
                process_df = df[df.FECHA.notnull()]
                df_final = process_df.fillna(0)
                reg_procesados, reg_insertados, reg_excluidos = save_registers_in_database(df_final, s.filename, formato, duplicados)
                good_files.append({'filename': s.filename, 'status': status_indiv_file, 'registros_procesados': reg_procesados, 'registros_insertados': reg_insertados,
                     'registros_excluidos': reg_excluidos, 'registros_duplicados_detalle': duplicados})
                total_registros_procesados += reg_procesados
                total_registros_insertados += reg_insertados
                total_registros_excluidos += reg_excluidos
            except AttributeError as e:
                indice = str(e).find('attribute')
                error = msg_error_column + str(e)[indice + 9:]
                bad_files.append(
                    {'file': s.filename, 'problema': error})
                save_file_upload_error(s.filename, error)


#return "tipo: "+tipo_archivo + " name_of_pc: " + name_of_pc + " formato: "+ formato + " filename: " + filename + " destitaion: " + destination
def process_excel_file(path_excel_file, filename, formato):
    global duplicados
    duplicados = []
    formato_excel = set_formato_excel(formato)
    try:
        app.logger.warning('destination: ' + path_excel_file )
        df = pd.read_excel(path_excel_file, converters=formato_excel)   
        process_df = df[df.FECHA.notnull()]
        df_final = process_df.fillna(0)
        reg_procesados, reg_insertados, reg_excluidos = save_registers_in_database(df_final, filename, formato, duplicados)
        return reg_procesados, reg_insertados, reg_excluidos
    except AttributeError as e:
        save_file_upload_error(filename, str(e))
        indice = str(e).find('attribute')
        global msg_error_column, status_indiv_file
        error = msg_error_column + str(e)[indice + 9:]
        status_indiv_file = "ERROR: " + error
        return 0


def save_registers_in_database(df, filename, formato, duplicados):
    reg_insertados = 0
    reg_procesados = 0
    conn = connect_database()
    cur = conn.cursor()
    save_data_for_auditoria(filename, cur)

    reg_excluidos = 0
    if formato == 1:
        for fila in df.itertuples():
            register = (fila.MONEDA, fila.DEPENDENCIA, fila.CONCEP, fila.a, fila.b,
                        fila.NUMERO, fila.CODIGO, fila.NOMBRE, fila.IMPORTE, fila.CARNET,
                        fila.AUTOSEGURO, fila.AVE, fila._13, fila.OBSERVACIONES, fila.FECHA)
            flag = save_register(register, cur, duplicados, filename)
            reg_procesados += 1
            if flag == 1:
                reg_insertados += 1
        conn.commit()
        conn.close()
    elif formato == 2:
        for fila in df.itertuples():
            register = (fila._1, fila.DEPENDENCIA, fila.CONCEP, fila.a, fila.b,
                        fila.NUMERO, fila.CODIGO, fila.NOMBRE, fila.IMPORTE, fila.CARNET,
                        fila.AUTOSEGURO, fila.AVE, fila._13, fila.OBSERVACIONES, fila.FECHA)
            flag = save_register(register, cur, duplicados, filename)
            reg_procesados += 1
            if flag == 1:
                reg_insertados += 1
        conn.commit()
        conn.close()
    reg_excluidos = reg_procesados - reg_insertados
    return reg_procesados, reg_insertados, reg_excluidos

#ENTENDIDO
def save_register(register, cur, duplicados,filename):
    print("existeeee" + str(existe(register, cur)))
    if not existe(register, cur):
        print ("entra existe")
        #GUARDA LOS DATOS DEL EXCEL EN LA TABLA RECAUDACIONES_RAW
        save_register_valid(register, cur) 
        #Obtiene el ultimo ID_RAW de la tabla RECUDACIONES_RAW
        cur.execute("SELECT id_raw FROM recaudaciones_raw ORDER BY id_raw DESC limit 1")
        id_rec = cur.fetchall()
        fecha_raw = register[14] 
        #DA FORMATO A LA FECHA
        fecha = dar_formato_fecha(fecha_raw) 
        #ACTUALIZA LA FECHA DE LA TABLA RECAUDACIONES SEGUN EL ID_REC
        save_recaudaciones_normalizada(fecha, id_rec[0], cur)
        return 1
    else:
        #pasa a cadena el arrreglo REGISTER y lo agregar al arreglo bidimensional DUPLICADOS
        duplicados.append({'registro': str(register)})
        return 0

#ENTENDIDO
#GUARDA EL REGITRO QUE NO EXISTE EN LA TABLA RECAUDACIONES_REW , GUARDA LOS DATOS DEL EXCEL XD
def save_register_valid(register, cur):
    query = "INSERT INTO recaudaciones_raw(moneda, dependencia, concep, concep_a, concep_b, numero, codigo, nombre, importe, carnet, autoseguro, ave, devol_tran, observacion, fecha) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.execute(query, register)

''' NO SE ENTIENDE
#ENTENDIDO
#GUARDA EL REGISTRO NORMALIZADO (FECHA) EN LA TABLA RECAUDACIONES
def save_recaudaciones_normalizada(fecha, id_rec, cur):
    query = "UPDATE recaudaciones SET fecha=%s WHERE id_rec=%s"
    update = (fecha, id_rec)
    cur.execute(query, update)
'''
#
#RUTA = FILENAME
#NAME_OF_PC = 
#IP =
#REIGSTRA LOS DATOS DEL EXCEL EN LA TABLA REGISTRO_CARGA
def save_data_for_auditoria(filename, cur):
    global name_of_pc, ip
    query = "INSERT INTO registro_carga(nombre_equipo, ip, ruta) VALUES(%s, %s, %s)"
    update = (name_of_pc, ip, filename)
    cur.execute(query, update)


def existe(register, cur):
    query = "SELECT count(*) FROM recaudaciones_raw where numero=%s;"
    data = (str(register[5]))
    cur.execute(query, data)
    flag = cur.fetchall()
    if int(flag[0][0]) == 0:
        return False
    else:
        query2 = "SELECT count(*) FROM recaudaciones_raw where moneda=%s AND dependencia=%s AND concep=%s AND concep_a=%s AND concep_b=%s AND codigo=%s AND nombre=%s AND importe=%s AND fecha=%s;"
        data2 = (register[0], register[1], register[2], register[3], register[4], register[6], register[7], str(register[8]), register[14])
        cur.execute(query2, data2)
        flag2 = cur.fetchall()
        if int(flag2[0][0])==0:
            register[5] = addzero(register[5])
            return False
        else:
            return True
    return True

def addzero(numero):
    print ("Numero cambiado")
    return "0"+str(numero)

def save_bad_files(self):
    return True


def save_file_upload_error(filename, error):
    try:
        conn = connect_database()
        cur = conn.cursor()
        query = "INSERT INTO recaudaciones_fallidas(nombre_archivo, descripcion_error) VALUES (%s, %s)"
        data = (filename, error)
        cur.execute(query, data)
        conn.commit()
        conn.close()
    except:
        print("I am unable to connect to the database.")


def set_formato_excel(formato):
    if formato == 1:
        return formato_one
    if formato == 2:
        return formato_two


def dar_formato_fecha(fecha_raw):
    return fecha_raw[:4] + '-' + fecha_raw[4:6] + '-' + fecha_raw[6:]


if __name__ == '__main__':
    #app.run(host="127.0.0.1")
    app.run()
