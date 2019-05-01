from flask import Flask, request, jsonify, json, redirect, url_for
from flask_cors import CORS
from zipfile import ZipFile
from helpers.campos_excel import formato_one, formato_two
import psycopg2 as ps
import pandas as pd
import os

app = Flask(__name__)
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
cors = CORS(app, resources={r"/*": {"origins": "*"}})
UPLOAD_FOLDER = '/static'
ALLOWED_EXTENSIONS = set(['xlsx'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

           
@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/hola', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return "NO HAY"
        if 'file' in request.files:
            file = request.files['file'] 
            return "file: " + file.filename

if __name__ == '__main__':
    app.run(host="127.0.0.1")