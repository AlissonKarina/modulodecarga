# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify, json, session, g
from flask_cors import CORS
from zipfile import ZipFile
from helpers.campos_excel import formato_one, formato_two
import psycopg2 as ps
import pandas as pd
import os

app = Flask(__name__)
app.secret_key = os.urandom(24)
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


# conexion = ps.conexionect(host="localhost", port=5432, dbname="tcs_prueba", user="postgres", password="1234")
# cursor = conexion.cursor()

conexion = ps.connect(host="67.205.143.180", port=5432, dbname="testcarga02", user="modulo4", password="modulo4")
cursor = conexion.cursor()


@app.route('/')
def hello_world():
    # query = "SELECT * FROM usuario;"
    # cursor.execute(query)
    # usuarios = cursor.fetchall()
    # for row in usuarios:
    #     print("id", row[0],row[1])
    # conexion.commit()
    # print(query)
    return 'Back de Módulo de carga, ready'

@app.route('/login', methods=['GET', 'POST'])
def index():
    result = False
    if request.method == 'POST':
        data = request.get_json()
        print(data)
        #username = str(data['username']+' ')
        username = data['username']
        password = data['password']
        cursor.execute("SELECT COUNT(*) FROM usuario where user_name = %s", (username,))
        validate = cursor.fetchall()
        print(validate)
        if int(validate[0][0] == 0):
            # NO EXISTE EL USUARIO
            result = 2
        else:
            cursor.execute("SELECT COUNT(*) FROM usuario where user_name=%s AND pass=%s", (username,password,))
            val_pass = cursor.fetchall()
            if int(val_pass[0][0]) != 0:
                result = True
            else:
                # CONTRASE'A EQUIVOCADA
                result = 3
    return jsonify(result)


@app.route('/upload', methods=['POST']) 
def upload():
    #TARGET = APP_ROOT + "static/"
    target = os.path.join(APP_ROOT, "static")

    #CHEQUEA SI EL ARCHIVO ESTÁ PRESENTE O NO
    if 'file' not in request.files:
        return "Not file found"
    
    print("ingreso file")

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
    ip = "172.16.64.133" #estatica
    formato = request.form.get('formato')

    print("tipo_archivo:" + tipo_archivo + "| name_of_pc:"+name_of_pc)
    
    #CREA UN ARRAY respuesta
    respuesta = {}

    #GUARDA EL NOMBRE DEL ARCHIVO EN filename, viene de HTML
    filename = file.filename

    #destination = target + filename
    #SE GUARDA EL ARCHIVO EXCEL EN LA CARPETA CREADA DE DIRECCION target
    destination = "/".join([target, filename])
    
    file.save(destination)

    if tipo_archivo == "zip":
        print("**************************** ARCHIVO ZIP **************************")
        print("**************************** ARCHIVO ZIP XDDD **************************")
        global total_registros_procesados, total_registros_insertados, total_registros_excluidos
        print("**************************** ARCHIVO ZIP XDDD DESPUES **************************")
        total_registros_procesados = 0
        total_registros_insertados = 0
        total_registros_excluidos = 0
        print("total_registros_procesados:" + str(total_registros_procesados) +"|total_registros_insertados:"+ str(total_registros_insertados) +"|total_registros_excluidos:"+ str(total_registros_excluidos))
        print("**************************** ANTES DEL PROCESS_ZIP_FILES **************************")
        process_zip_file(destination, filename, int(formato))
        global good_files, bad_files, duplicados
        respuesta = {'file': filename, 'good_files': {'lista_detalle': good_files, 'total_registros_procesados': total_registros_procesados, 'total_registros_insertados': total_registros_insertados,
                     'total_registros_excluidos': total_registros_excluidos}, 'bad_files': bad_files}
        #os.remove(destination)
        return jsonify(respuesta) 
    if tipo_archivo == "excel":
        #global duplicados
        reg_procesados, reg_insertados, reg_excluidos = process_excel_file(destination, filename, int(formato))
        respuesta = {'filename': filename, 'status': status_indiv_file, 'registros_procesados': reg_procesados, 'registros_insertados': reg_insertados,
                     'registros_excluidos': reg_excluidos, 'registros_duplicados_detalle': duplicados}
        os.remove(destination)
        return jsonify(respuesta)

#
#path_zip_file = DESTINATION
#filename = NOMBRE DEL EXCEL
#formato= TIPO DE FORMATO
def process_zip_file(path_zip_file, filename, formato):
    print("Entro al process_zip_file")
    global total_registros_procesados, total_registros_insertados, total_registros_excluidos, msg_error_column, good_files, bad_files, duplicados
    formato_excel = set_formato_excel(formato) #OBTIENE EL TIPO DE FORMATO
    print("formato_excel:" + str(formato_excel))
    archivo_zip = ZipFile(path_zip_file, 'r')
    content_of_zip = archivo_zip.infolist() #CONTENIDO DEL ZIP, ES DECIR UNA LISTA DE EXCEL
    print(content_of_zip)
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
                return 0


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
        error = msg_error_column + str(e)[indice + 9:] + " es el indice " + str(indice)
        status_indiv_file = "ERROR: " + error
        return 0


def save_registers_in_database(df, filename, formato, duplicados):
    reg_insertados = 0
    reg_procesados = 0

    save_data_for_auditoria(filename, cursor)

    reg_excluidos = 0
    
    if formato == 1:
        for fila in df.itertuples():
            register = (fila.MONEDA, fila.DEPENDENCIA, fila.CONCEP, fila.a, fila.b,
                        fila.NUMERO, fila.CODIGO, fila.NOMBRE, fila.IMPORTE, fila.CARNET,
                        fila.AUTOSEGURO, fila.AVE, fila._13, fila.OBSERVACIONES, fila.FECHA)
            flag = save_register(register, cursor, duplicados, filename)
            reg_procesados += 1
            if flag == 1:
                reg_insertados += 1
        conexion.commit()
    elif formato == 2:
        for fila in df.itertuples():
            register = (fila._1, fila.DEPENDENCIA, fila.CONCEP, fila.a, fila.b,
                        fila.NUMERO, fila.CODIGO, fila.NOMBRE, fila.IMPORTE, fila.CARNET,
                        fila.AUTOSEGURO, fila.AVE, fila._13, fila.OBSERVACIONES, fila.FECHA)
            flag = save_register(register, cursor, duplicados, filename)
            reg_procesados += 1
            if flag == 1:
                reg_insertados += 1
        conexion.commit()
    reg_excluidos = reg_procesados - reg_insertados
    return reg_procesados, reg_insertados, reg_excluidos

#ENTENDIDO
def save_register(register, cursor, duplicados,filename):
    flag = True
    
    while flag:
        opcion = existe(register, cursor)

        if opcion == 0:
            save_register_valid(register, cursor)
            flag = False
            return 1
        else: 
            if  opcion == 2:
                register = addzero(register)
                flag = True
            else:
                duplicados.append({'registro': str(register)})
                flag = False
                return 0

#ENTENDIDO
#GUARDA EL REGITRO QUE NO EXISTE EN LA TABLA RECAUDACIONES_REW , GUARDA LOS DATOS DEL EXCEL XD
def save_register_valid(register, cursor):
    query = "INSERT INTO recaudaciones_raw(moneda, dependencia, concep, concep_a, concep_b, numero, codigo, nombre, importe, carnet, autoseguro, ave, devol_tran, observacion, fecha) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, register)

''' NO SE ENTIENDE
#ENTENDIDO
#GUARDA EL REGISTRO NORMALIZADO (FECHA) EN LA TABLA RECAUDACIONES
def save_recaudaciones_normalizada(fecha, id_rec, cursor):
    query = "UPDATE recaudaciones SET fecha=%s WHERE id_rec=%s"
    update = (fecha, id_rec)
    cursor.execute(query, update)
'''
#
#RUTA = FILENAME
#NAME_OF_PC = 
#IP =
#REIGSTRA LOS DATOS DEL EXCEL EN LA TABLA REGISTRO_CARGA
def save_data_for_auditoria(filename, cursor):
    global name_of_pc, ip
    query = "INSERT INTO registro_carga(nombre_equipo, ip, ruta) VALUES(%s, %s, %s)"
    update = (name_of_pc, ip, filename)
    cursor.execute(query, update)


def existe(register, cursor):
    print("****************** EXISTE () !!! ******************")
    query_recraw = "SELECT count(*) FROM recaudaciones_raw where numero=%s"
    data_recraw = (str(register[5]),)

    cursor.execute(query_recraw, data_recraw)
    flag_recraw = cursor.fetchall()
    if int(flag_recraw[0][0]) == 0:
        print("rec1-raw - no existe")
        return ver_recaudaciones(register, cursor)
    else:
        query = "SELECT count(*) FROM recaudaciones_raw where moneda=%s AND concep=%s AND numero=%s AND nombre=%s AND importe=%s AND fecha=%s;"
        data = (register[0], register[2], str(register[5]),  register[7], str(register[8]), register[14])
        cursor.execute(query, data)
        flag = cursor.fetchall()
        if int(flag[0][0]) == 0:
            print("rec1-raw - mimo numero-campos-diferentes")
            return ver_recaudaciones(register, cursor)
        else:
            print("rec1-raw - duplicado")
            return 1


def ver_recaudaciones(register, cursor):
    query_rec = "SELECT count(*) FROM recaudaciones WHERE numero=%s"
    data_rec = (str(register[5]),)

    cursor.execute(query_rec, data_rec)
    flag_rec = cursor.fetchall()

    if int(flag_rec[0][0]) == 0:
        print("NUMERO DIFERENTE, RETURN 0")
        print("facultad: " + str(register[1]))
        print("numero: " + str(register[5]))
        return 0
    else:
        query_rec2 = "select count(*) from recaudaciones r INNER JOIN concepto c on r.id_concepto = c.id_concepto INNER JOIN alumno a on a.id_alum = r.id_alum INNER JOIN facultad f on f.id_facultad = a.id_facultad WHERE  r.moneda=%s AND c.concepto=%s AND numero=%s AND a.ape_nom=%s AND r.importe=%s AND r.fecha=%s;"
        data_rec2 = (register[0], register[2],  str(register[5]),   register[7], str(register[8]), register[14])
        cursor.execute(query_rec2, data_rec2)
        flag_rec2 = cursor.fetchall()

        if int(flag_rec2[0][0])==0:
            print("IGUAL NUMERO, PERO DIFERENTES CAMPOS, RETURN 2")
            print("numero: " + str(register[5]))
            print("facultad: " + str(register[1]))
            return 2
        else:
            print("CAMPOS IGUALES, RETURN 1")
            print("numero: " + str(register[5]))
            print("facultad: " + str(register[1]))
            return 1
        #query_recraw2 = "SELECT count(*) FROM recaudaciones_raw where moneda=%s AND dependencia=%s AND concep=%s AND concep_a=%s AND concep_b=%s AND codigo=%s AND nombre=%s AND importe=%s AND fecha=%s;"
        #data_recraw2 = (register[0], register[1], register[2], register[3], register[4], register[6], register[7], str(register[8]), register[14])
        #cursor.execute(query_recraw2, data_recraw2)
        #flag_recraw2 = cursor.fetchall()
        #if int(flag_recraw2[0][0])==0:
        #    return 2
        #else:
        #    return 1
        

def addzero(register):
    return (register[0], register[1], register[2], register[3], register[4],'0'+register[5] , register[6], register[7], str(register[8]),register[9] , register[10], register[11], register[12], register[13], str(register[14]))

def save_bad_files(self):
    return True


def save_file_upload_error(filename, error):
    try:
        #conexion = connect_database()
        #cursor = conexion.cursorsor()
        query = "INSERT INTO recaudaciones_fallidas(nombre_archivo, descripcion_error) VALUES (%s, %s)"
        data = (filename, error)
        cursor.execute(query, data)
        conexion.commit()
        conexion.close()
    except:
        print("I am unable to connect to the database.")


def set_formato_excel(formato):
    if formato == 1:
        return formato_one
    if formato == 2:
        return formato_two


def dar_formato_fecha(fecha_raw):
    return fecha_raw[:4] + '-' + fecha_raw[4:6] + '-' + fecha_raw[6:]


# @app.before_request
# def before_request():
#     g.user = None
#     if 'user' in session:
#         g.user = session['user']

if __name__ == '__main__':
#    app.run(host="127.0.0.1")
     app.run()
#    app.run()
