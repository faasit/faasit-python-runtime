import flask
import time
import tempfile
app = flask.Flask(__name__)

@app.post('/')
def recv_data():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        try:
            while True:
                chunk = flask.request.stream.read(8192)
                time.sleep(0.5)
                if not chunk:
                    break
                f.write(chunk)
        finally:
            f.close()
    return flask.jsonify({"status": "ok", "filename": f.name})

app.run(host='0.0.0.0', port=8080)