from flask import Flask, request, abort, jsonify
from tasks import load_data, process_data, perform_cluster
from datetime import datetime
from celery import uuid
import celery

app = Flask(__name__)

current_tasks = {}


@app.route("/cluster", methods=["POST"])
def post_cluster_task():
    if not request.json:
        abort(400)
    try:
        mq = request.json["mongo_query"]
        cp = request.json["clustering_params"]
    except KeyError:
        abort(400)
    data = process_data(
        load_data(
            mq, {'addr': '10.0.0.222', 'port': 27017}
        )
    )
    tid = uuid()
    t = perform_cluster.apply_async((data, cp), task_id=tid)

    current_tasks[tid] = t
    return jsonify({"id": tid}), 200


@app.route("/cluster/<string:task_id>", methods=["GET"])
def get_cluster_task(task_id):
    if task_id not in current_tasks:
        abort(404)
    if celery.result.AsyncResult(task_id).status == "SUCCESS":
        return jsonify({"result": current_tasks[task_id].get, "status": "SUCCESS"}), 200
    else:
        return jsonify({"status": celery.result.AsyncResult(task_id).status})


app.run("0.0.0.0", 8081)



