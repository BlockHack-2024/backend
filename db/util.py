import MySQLdb
from flask import jsonify
from data.dataset import read_dataset, datasets

# MySQL configurations
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'changeit'
MYSQL_DB = 'blockhacks2024'

def get_db_connection():
    return MySQLdb.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        db=MYSQL_DB
    )

def create_table():
    db = get_db_connection()
    cursor = db.cursor()

    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255),
            password VARCHAR(255),
            validator BIT(1)
        )
    """)
    
    # Create predictions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255),
            result TEXT,
            time_span BIGINT,
            task_id BIGINT
        )
    """)

    # Create tasks table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            miner_id BIGINT,
            validator_id BIGINT,
            task_name VARCHAR(255),
            task_description TEXT,
            dataset TEXT
        )
    """)

    db.commit()
    cursor.close()
    db.close()

def register(username: str, password: str, validator: bool):
    validator_bit = 1 if validator else 0
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("INSERT INTO users (username, password, validator) VALUES (%s, %s, %s)", (username, password, validator_bit))
    db.commit()
    cursor.close()
    db.close()
    return jsonify({"response": "User registered successfully"}), 201

def login(username: str, password: str):
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT password FROM users WHERE username = %s", [username])
    user = cursor.fetchone()
    cursor.close()
    db.close()
    if user and user[0] == password:
        return jsonify({"response": "Login successful"}), 200
    else:
        return jsonify({"response" : "Invalid username or password"}), 401
    
def get_assigned_tasks_miner(user_id):
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM tasks WHERE miner_id = %s", [user_id])
    tasks = cursor.fetchall()
    formatted_tasks = []
    for task in tasks:
        task_id, miner_id, validator_id, task_name, task_description, raw_dataset = task
        formatted_task = {
            'task_id': task_id,
            'miner_id': miner_id,
            'validator_id': validator_id,
            'task_name': task_name,
            'task_description': task_description,
            'dataset': raw_dataset
        }
        formatted_tasks.append(formatted_task)
    cursor.close()
    db.close()
    return jsonify({"response": formatted_task}), 200

def get_available_tasks():
    return jsonify({"response": datasets}), 200

def assign_task(task):
    miner_id = task['miner_id']
    validator_id = task['validator_id']
    task_name = task['task_name']
    task_description = task['task_description']
    dataset_id = task['dataset_id']
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute('''
                INSERT INTO tasks (miner_id, validator_id, task_name, task_description, dataset)
                VALUES (%s, %s, %s, %s, %s)
            ''', (miner_id, validator_id, task_name, task_description, read_dataset(dataset_id).to_string()))
    db.commit()
    cursor.close()
    db.close()
    return jsonify({"response": "task is assigned"}), 200