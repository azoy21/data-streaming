from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

def get_data():
    conn = psycopg2.connect(
        host="postgres",
        database="datastream",
        user="user",
        password="password"
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM data ORDER BY id DESC LIMIT 20")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"id": row[0], "timestamp": row[1], "value": row[2]} for row in rows]

@app.route('/data', methods=['GET'])
def data():
    return jsonify(get_data())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
