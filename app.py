from flask import Flask, request, jsonify
from data.dataset import read_dataset
from db.util import *

app = Flask(__name__)

app.debug = True

create_table()

@app.route("/signup", methods=["POST"])
def signup():
    data = request.get_json()
    print(data)
    username = data['username']
    password = data['password']
    validator = data['validator']
    return jsonify({"response": register(username, password, validator)}), 201

@app.route("/login", methods=["POST"])
def authenticate():
    data = request.get_json()
    username = data['username']
    password = data['password']
    status = login(username, password)
    return jsonify({"response": "Login successful"}), 201 if status == "Login successful" else jsonify({"response" : "Invalid username or password"}), 401

@app.route("/get_available_tasks", methods=["GET"])
def get_tasks_for_validator():
    return jsonify({"response": get_available_tasks()}), 200

@app.route("/get_assigned_tasks/<user_id>", methods=["GET"])
def get_tasks_for_miner(user_id):
    return jsonify({"response": get_assigned_tasks_miner(user_id)}), 200

@app.route("/assign_task", methods=["POST"])
def add_task():
    task = request.get_json()
    return jsonify({"response": assign_task(task)}), 200

@app.route("/predict", methods=["POST"])
def make_prediction():
    prediction = request.get_json()
    return jsonify({"response": insert_prediction(prediction)}), 200

@app.route("/get_posted_tasks/<validator_id>", methods=["GET"])
def get_posted_tasks(validator_id):
    return jsonify({"response": get_all_tasks(validator_id)}), 200

@app.route("/get_predictions/<task_id>", methods=["GET"])
def get_predictions(task_id):
    return jsonify({"response": get_all_predictions(task_id)}), 200

if __name__ == "__main__":
    app.run()