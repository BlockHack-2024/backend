from flask import Flask, request, jsonify
from data.dataset import read_dataset
from db.util import create_table, register, login, get_available_tasks, get_assigned_tasks_miner, assign_task

app = Flask(__name__)

app.debug = True

create_table()

@app.route("/signup", methods=["POST"])
def signup():
    data = request.get_json()
    username = data['username']
    password = data['password']
    validator = data['validator']
    return register(username, password, validator)

@app.route("/login", methods=["POST"])
def authenticate():
    data = request.get_json()
    username = data['username']
    password = data['password']
    return login(username, password)

@app.route("/get_available_tasks", methods=["GET"])
def get_tasks_for_validator():
    return get_available_tasks()

@app.route("/get_assigned_tasks/<user_id>", methods=["GET"])
def get_tasks_for_miner(user_id):
    return get_assigned_tasks_miner(user_id)

@app.route("/assign_task", methods=["POST"])
def add_task():
    task = request.get_json()
    return assign_task(task)

if __name__ == "__main__":
    app.run()