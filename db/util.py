import MySQLdb
from flask import jsonify
from data.dataset import read_dataset, datasets
import random
from datetime import datetime

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

# run to init tables in local db
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
    # batch prediction = {1: 1 day, 2: 1 week, 3: 1 month}
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            result DOUBLE,
            miner_id BIGINT,
            validator_id BIGINT,
            task_id BIGINT,
            batch INT,
            time_stamp DATE,
            loss DOUBLE
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

# register user with username password, and validator to indicate if user is a validator
def register(username, password, validator):
    validator_bit = 0
    if validator == 'true':
        validator_bit = 1
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("INSERT INTO users (username, password, validator) VALUES (%s, %s, %s)", (username, password, validator_bit))
    db.commit()
    cursor.close()
    db.close()
    return "User registered successfully"

# login with credential
def login(username, password):
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT password FROM users WHERE username = %s", [username])
    user = cursor.fetchone()
    cursor.close()
    db.close()
    if user and user[0] == password:
        return "Login successful"
    else:
        return "Invalid username or password"

# given a user id for miner, fetch all tasks assigned to them in the format:
"""
tasks = [
{
    'task_id': task_id,
    'miner_id': miner_id,
    'validator_id': validator_id,
    'task_name': task_name,
    'task_description': task_description,
    'dataset': raw_dataset
},
{
    'task_id': task_id2,
    'miner_id': miner_id2,
    'validator_id': validator_id2,
    'task_name': task_name2,
    'task_description': task_description2,
    'dataset': raw_dataset2
}
]
"""
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
    return formatted_tasks

# get all potential tasks that can be used
def get_available_tasks():
    return datasets

# assign a task to a miner, the task is in the format example:
"""
{
    "miner_id": 2,
    "validator_id": 1,
    "task_name": "Employment Level - Bachelor's Degree and Higher, 25 Yrs. & over",
    "task_description": "Predict Employment Level for Bachelor's Degree and Higher, 25 Yrs. & over for the next week/month/year",
    "dataset_id": "LNS12027662"
}
"""
# note the dataset_id should match one of the task_ids in dataset.py
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
    return "Task is assigned"

# insert a prediction into the database, prediction is in the format:
"""
{
    "result": "0.1134",
    "miner_id": 2,
    "validator_id": 1,
    "task_id": 1,
    "batch": 1
}
"""
def insert_prediction(prediction):
    result = prediction['result']
    miner_id = prediction['miner_id']
    validator_id = prediction['validator_id']
    task_id = prediction['task_id']
    batch = prediction['batch']

    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute('''
                INSERT INTO predictions (result, miner_id, validator_id, task_id, batch, time_stamp, loss)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (result, miner_id, validator_id, task_id, batch, datetime.today().strftime('%Y-%m-%d'), random.random()))
    db.commit()
    cursor.close()
    db.close()
    return "Prediction is inserted"


# get all tasks posted by the validator using validator_id, result is of the format:
"""
[
{
    'miner_id': miner_id,
    'validator_id': validator_id,
    'task_name': task_name,
    'task_description': task_description
},
{
    'miner_id': miner_id2,
    'validator_id': validator_id2,
    'task_name': task_name2,
    'task_description': task_description2
}
]
"""
def get_all_tasks(validator_id):
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM tasks WHERE validator_id = %s", [validator_id])
    tasks = cursor.fetchall()
    formatted_tasks = []
    for task in tasks:
        _, miner_id, validator_id, task_name, task_description, _, = task
        formatted_task = {
            'miner_id': miner_id,
            'validator_id': validator_id,
            'task_name': task_name,
            'task_description': task_description
        }
        formatted_tasks.append(formatted_task)
    cursor.close()
    db.close()
    return formatted_tasks


# get all predictions made by the miners for a specific task with task_id, the result is like:
"""
[{
    'result': result,
    'miner_id': miner_id,
    'validator_id': validator_id,
    'task_id': task_id,
    'batch': batch,
    'time_stamp': time_stamp,
    'loss': loss
},
{
    'result': result2,
    'miner_id': miner_id2,
    'validator_id': validator_id2,
    'task_id': task_id2,
    'batch': batch2,
    'time_stamp': time_stamp2,
    'loss': loss2
}]
"""
def get_all_predictions(task_id):
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM predictions WHERE task_id = %s", [task_id])
    predictions = cursor.fetchall()
    formated_predictions = []
    for prediction in predictions:
        _, result, miner_id, validator_id, task_id, batch, time_stamp, loss = prediction
        formatted_task = {
            'result': result,
            'miner_id': miner_id,
            'validator_id': validator_id,
            'task_id': task_id,
            'batch': batch,
            'time_stamp': time_stamp,
            'loss': loss
        }
        formated_predictions.append(formatted_task)
    cursor.close()
    db.close()
    return formated_predictions
