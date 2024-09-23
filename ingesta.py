import boto3
import mysql.connector
import pandas as pd


try: 
    connection = mysql.connector.connect(host='mysql',
                                         port='8080',
                                         database='db_nombres',
                                         user='mysql',
                                         password='mysql',
                                         )
    sql_select_Query = "select * from datos"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    df = pd.DataFrame(records)
    ficheroUpload = 'datos.csv'
    df.to_csv(ficheroUpload, index=False)
    cursor.close()
    connection.close()


except:
    print("Error de conexión a la base de datos")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Conexión cerrada")

s3 = boto3.client('s3')
response = s3.upload_file(ficheroUpload, 'data', ficheroUpload)

print("Ingesta completada")
