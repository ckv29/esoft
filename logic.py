import pandas
from db_working import create_table, delete_table,save_results, extract_data, delete_data_from_analytics
import io
import psycopg2
import os
import json
from werkzeug.utils import secure_filename

def import_file(file,filename):
    try:
        file_stream = io.BytesIO(file.read())

        if filename.endswith('.csv'):
            df = pandas.read_csv(file_stream)
        elif filename.endswith(('.xls', '.xlsx')):
            df = pandas.read_excel(file_stream)
        else:
            raise ValueError("Unsupported file format. Use .csv, .xls or .xlsx")
        # Заменяем NaN на None (для корректной вставки в PostgreSQL)
        df = df.where(pandas.notnull(df), None)      
        print('precreate_table')  
        #analitics = make_analytics(df)
        create_table(filename, df.columns.tolist(), df)
        
        analytics = make_analytics(df)
        print(' after analytics ')
        print(analytics)
        res = save_results(filename,analytics)
        #print(f'save_results res - {res}')
        return "ok",201
    except Exception as e:
        return f'error : {str(e)}', 500    
    return res


def get_analytics(filename):
    try:
        analytics = extract_data(filename)
        if analytics is None:
            return " Нет аналитики для этого файла ", 404
        return json.dumps(analytics, ensure_ascii=False, indent=2)
    except Exception as e:
        return f' error : {e}',500
    


def make_analytics(df):        
    numeric_df = df.select_dtypes(include='number')    
    if numeric_df.empty:
        return {
            'mean': None,
            'median': None,
            'correlation': None,
            'message': 'No numeric columns found for analysis.'
        }

    mean_vals = numeric_df.mean().to_dict()
    median_vals = numeric_df.median().to_dict()

    # Для корреляции — делаем то же самое
    correlation_m = numeric_df.corr()
    # Заменяем NaN на None и конвертируем в dict с нативными типами
    correlation_m = correlation_m.where(pandas.notnull(correlation_m), None).to_dict()
    analytics =  {
        'mean' : mean_vals,
        'median' : median_vals,
        'corr' : correlation_m
    }

    return convert_numpy_types(analytics)
    
    
def convert_numpy_types(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(v) for v in obj]
    elif hasattr(obj, 'item'):  # numpy scalars
        return obj.item()
    elif obj is pandas.NA or (obj is not None and pandas.isna(obj)):
        return None
    else:
        return obj


def delete_filedata(filename):
    res = ''
    res = delete_table(filename) 
    if res != 'ok':
        return res,500
    res = delete_data_from_analytics(filename)
    if res != 'ok':
        return res,500
    return 'ok',204    
    