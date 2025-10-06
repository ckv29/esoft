from flask import Flask
from flask import request
import flask
from logic import import_file, delete_filedata,get_analytics


app = Flask(__name__)

API_ROOT = "/api/v1"

class ApiException(Exception):
    pass

# загружаем файл, указывая его имя
@app.route(API_ROOT + "/upload/<filename>/", methods = ["POST"])
def load(filename):
    if 'file' not in request.files:
        return f'error: No file', 400

    file = request.files['file']
    if file.filename == '':
        return f'error: No filename', 400

    filename = file.filename
    print('filename - ' + filename)
    if not (filename.endswith('.csv') or filename.endswith(('.xls', '.xlsx'))):
        return f"error: Wrong filename, use .csv, .xls or .xlsx", 415
    import_file(file,filename)
    return "ok",201



# запрашиваем аналитику
@app.route(API_ROOT + "/data/status/<filename>/", methods = ["GET"])
def analize(filename):
    return get_analytics(filename)


# удаляем файл из базы
@app.route(API_ROOT + "/data/clean/<filename>/", methods = ["GET"])
def delete(filename):
    res = delete_filedata(filename)
    return res
