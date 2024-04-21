import json
import os
import random
import sqlite3
from flask import Flask, render_template, request, redirect, url_for
from google.cloud import pubsub_v1

# Flask setup
app = Flask(__name__)

# The images of words we want to test
test_images = os.listdir("static/images/cropped_words")
f = open("static/ocr_results.json")
ocr_results = json.load(f)

# GCP setup
PROJECT_ID = "dcda-ocr"
TOPIC_NAME = "dcda-io-votes"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


# Database setup
def get_db_connection():
    conn = sqlite3.connect("responses.db")
    conn.row_factory = sqlite3.Row
    return conn


# Initialize the database (run this once to create the table)
def init_db():
    conn = get_db_connection()
    with open("schema.sql", "r") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()


@app.route("/")
def index():
    chosen_image = random.choice(test_images)
    image_locator = chosen_image.split(".")[0].split("_")[0]
    word_in_image = chosen_image.split(".")[0].split("_")[1]
    chosen_word = ocr_results[int(image_locator) - 1]["words"][int(word_in_image) - 1][
        "text"
    ]
    return render_template(
        "index.html", render_image=chosen_image, render_word=chosen_word
    )


@app.route("/vote", methods=["POST"])
def vote():
    image_file = request.form["word_voted_on"]
    vote_type = request.form["vote"]

    message_data = {
        "image_file": image_file,
        "vote_type": vote_type,
    }
    # Convert the message data to bytestring
    data = json.dumps(message_data).encode("utf-8")

    # Publish the message
    future = publisher.publish(topic_path, data)

    # insert the vote into the database
    conn = get_db_connection()
    cursor = conn.execute(
        "INSERT INTO votes (image_file, direction) VALUES (?, ?)",
        (image_file, vote_type),
    )

    conn.commit()
    conn.close()

    return redirect(url_for("index"))


if __name__ == "__main__":
    # init_db()  # Initialize database on first run
    server_port = os.environ.get("PORT", "8080")
    app.run(debug=True, port=server_port, host="0.0.0.0")
