import argparse
from flask import Flask, render_template, request, redirect, url_for, Request, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from pathlib import Path
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage

parser = argparse.ArgumentParser("simple flask app")
parser.add_argument("--db-address", help="the address of the rqlite database", default="localhost")
parser.add_argument("--db-port", help="the  of the rqlite database", default="4001")

subparsers = parser.add_subparsers(dest="cmd", required=True)

subparsers.add_parser("initdb", help="initialize the database")
run_parser = subparsers.add_parser("run", help="run the app")
args = parser.parse_args()

app = Flask(__name__)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"echo": True}
app.config["SQLALCHEMY_DATABASE_URI"] = f"rqlite+pyrqlite://{args.db_address}:{args.db_port}/"
app.config["UPLOAD_FOLDER"] = "uploads"


db = SQLAlchemy(app)

request: Request


class Img(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255))


@app.route("/", methods=["get"])
def root_get():
    return render_template(
        "index.html",
        images_url=url_for("download_image", filename=""),
        images=Img.query.order_by(Img.id.desc()).limit(64),
    )


@app.route("/images/<filename>")
def download_image(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)


@app.route("/", methods=["post"])
def root_post():
    file: FileStorage = request.files.get("file", None)
    if not file:
        return render_template("error.html", back_url=url_for("root_get"))

    filename = secure_filename(file.filename)
    filepath = Path(app.config["UPLOAD_FOLDER"]) / filename
    file.save(filepath)

    db.session.add(Img(filename=filename))
    db.session.commit()
    return redirect(url_for("root_get"))


if args.cmd == "initdb":
    db.create_all()
elif args.cmd == "run":
    app.run(host="0.0.0.0")
