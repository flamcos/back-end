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
                participantes TEXT,
                data_hora_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP            
            )
        """)
    except Exception as e:
        raise Exception(f"Erro ao criar tabela 'reunioes': {e}")

def create_avaliacoes_table_if_not_exists(cur):
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS avaliacoes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                reuniao_id INT NOT NULL,
                nota INT NOT NULL CHECK (nota BETWEEN 1 AND 5),
                descricao TEXT,
                email VARCHAR(255) NOT NULL,
                data_hora_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (reuniao_id) REFERENCES reunioes(id)
            )
        """)
    except Exception as e:
        raise Exception(f"Erro ao criar tabela 'avaliacoes': {e}")

def connect_db_and_ensure_tables(db_host, db_user, db_pass, db_name, db_port):
    try:
        conn = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
            port=db_port,
            connect_timeout=5
        )
    except Exception as e:
        raise Exception(f"Erro ao conectar ao banco de dados: {e}")

    try:
        with conn.cursor() as cur:
            create_reunioes_table_if_not_exists(cur)
            create_avaliacoes_table_if_not_exists(cur)
    except Exception as e:
        conn.close()
        raise
    return conn

def lambda_handler(event, context):
    secret_arn = os.environ['DB_SECRET_ARN']
    region = os.environ['AWS_REGION']
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']

    db_credentials = get_db_credentials(secret_arn, region)
    db_host = db_credentials['host']
    db_name = db_credentials['dbname']
    db_user = db_credentials['username']
    db_pass = db_credentials['password']
    db_port = int(db_credentials.get('port', 3306))

    body = json.loads(event['body'])
    reuniao = {
        'nome': body['nome'],
        'data_hora': body['data_hora'],
        'lista_participantes': body['lista_participante']
    }

    try:
        conn = connect_db_and_ensure_table(db_host, db_user, db_pass, db_name, db_port)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO reunioes (nome, data_hora, participantes) VALUES (%s, %s, %s)",
                (reuniao['nome'], reuniao['data_hora'], reuniao['lista_participantes'])
            )
            conn.commit()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'message': str(e)})
        }
    finally:
        if 'conn' in locals():
            conn.close()

    return {
        'statusCode': 201,
        'body': json.dumps({'status': 'success'})
    }