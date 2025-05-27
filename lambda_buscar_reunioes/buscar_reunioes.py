import pymysql
import boto3
import json
import os

def get_db_credentials(secret_arn, region):
    client = boto3.client('secretsmanager', region_name=region)
    secret = client.get_secret_value(SecretId=secret_arn)
    return json.loads(secret['SecretString'])

def create_reunioes_table_if_not_exists(cur):
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reunioes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                data_hora DATETIME NOT NULL,
                participantes TEXT
            )
        """)
    except Exception as e:
        raise Exception(f"Erro ao criar tabela: {e}")

def connect_db_and_ensure_table(db_host, db_user, db_pass, db_name, db_port):
    try:
        conn = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
            port=db_port,
            connect_timeout=5
        )
        with conn.cursor() as cur:
            create_reunioes_table_if_not_exists(cur)
        return conn
    except Exception as e:
        raise Exception(f"Erro ao conectar ou criar tabela: {e}")

def lambda_handler(event, context):
    secret_arn = os.environ['DB_SECRET_ARN']
    region = os.environ['AWS_REGION']

    db_credentials = get_db_credentials(secret_arn, region)
    db_host = db_credentials['host']
    db_name = db_credentials['dbname']
    db_user = db_credentials['username']
    db_pass = db_credentials['password']
    db_port = int(db_credentials.get('port', 3306))

    try:
        conn = connect_db_and_ensure_table(db_host, db_user, db_pass, db_name, db_port)
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT id, nome, data_hora, participantes FROM reunioes")
            reunioes = cur.fetchall()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'message': str(e)})
        }
    finally:
        if 'conn' in locals():
            conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(reunioes)
    }