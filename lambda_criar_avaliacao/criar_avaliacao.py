import pymysql
import boto3
import json
import os

def get_db_credentials(secret_arn, region):
    client = boto3.client('secretsmanager', region_name=region)
    secret = client.get_secret_value(SecretId=secret_arn)
    return json.loads(secret['SecretString'])

def create_reunioes_table_if_not_exists(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reunioes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            data_hora DATETIME NOT NULL,
            participantes TEXT
        )
    """)

def create_avaliacoes_table_if_not_exists(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS avaliacoes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reuniao_id INT NOT NULL,
            nota INT NOT NULL CHECK (nota BETWEEN 1 AND 5),
            descricao TEXT,
            email VARCHAR(255) NOT NULL,
            data_avaliacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (reuniao_id) REFERENCES reunioes(id)
        )
    """)

def connect_db_and_ensure_tables(db_host, db_user, db_pass, db_name, db_port):
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
        create_avaliacoes_table_if_not_exists(cur)
    return conn

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
        body = json.loads(event['body'])
        reuniao_id = int(body['reuniao_id'])
        nota = int(body['nota'])
        descricao = body.get('descricao', '')
        email = body['email']
        if nota < 1 or nota > 5:
            raise ValueError("Nota deve ser entre 1 e 5")
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'status': 'error', 'message': f'Dados inválidos: {e}'})
        }

    try:
        conn = connect_db_and_ensure_tables(db_host, db_user, db_pass, db_name, db_port)
        with conn.cursor() as cur:
            # Verifica se a reunião existe
            cur.execute("SELECT id FROM reunioes WHERE id = %s", (reuniao_id,))
            if not cur.fetchone():
                return {
                    'statusCode': 404,
                    'body': json.dumps({'status': 'error', 'message': 'Reunião não encontrada'})
                }
            # Insere avaliação
            cur.execute(
                "INSERT INTO avaliacoes (reuniao_id, nota, descricao, email) VALUES (%s, %s, %s, %s)",
                (reuniao_id, nota, descricao, email)
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
        'body': json.dumps({'status': 'success', 'message': 'Avaliação registrada com sucesso'})
    }