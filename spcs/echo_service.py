# from flask import Flask
# from flask import request
# from flask import make_response
# from flask import render_template
# import logging
# import os
# import sys

# SERVICE_HOST = os.getenv('SERVER_HOST', '0.0.0.0')
# SERVICE_PORT = os.getenv('SERVER_PORT', 8080)
# CHARACTER_NAME = os.getenv('CHARACTER_NAME', 'I')
# POSTGRES_URL = os.getenv('POSTGRES_URL')


# def get_logger(logger_name):
#     logger = logging.getLogger(logger_name)
#     logger.setLevel(logging.DEBUG)
#     handler = logging.StreamHandler(sys.stdout)
#     handler.setLevel(logging.DEBUG)
#     handler.setFormatter(
#         logging.Formatter(
#             '%(name)s [%(asctime)s] [%(levelname)s] %(message)s'))
#     logger.addHandler(handler)
#     return logger


# logger = get_logger('echo-service')

# app = Flask(__name__)


# @app.get("/healthcheck")
# def readiness_probe():
#     return "I'm ready!"


# @app.post("/echo")
# def echo():
#     '''
#     Main handler for input data sent by Snowflake.
#     '''
#     message = request.json
#     logger.debug(f'Received request: {message}')

#     if message is None or not message['data']:
#         logger.info('Received empty message')
#         return {}

#     # input format:
#     #   {"data": [
#     #     [row_index, column_1_value, column_2_value, ...],
#     #     ...
#     #   ]}
#     input_rows = message['data']
#     logger.info(f'Received {len(input_rows)} rows')

#     # output format:
#     #   {"data": [
#     #     [row_index, column_1_value, column_2_value, ...}],
#     #     ...
#     #   ]}
#     output_rows = [[row[0], get_echo_response(row[1])] for row in input_rows]
#     logger.info(f'Produced {len(output_rows)} rows')

#     response = make_response({"data": output_rows})
#     response.headers['Content-type'] = 'application/json'
#     logger.debug(f'Sending response: {response.json}')
#     return response


# @app.route("/ui", methods=["GET", "POST"])
# def ui():
#     '''
#     Main handler for providing a web UI.
#     '''
#     if request.method == "POST":
#         # getting input in HTML form
#         input_text = request.form.get("input")
#         # display input and output
#         return render_template("basic_ui.html",
#             echo_input=input_text,
#             echo_reponse=get_echo_response(input_text))
#     return render_template("basic_ui.html")


# def get_echo_response(input):
#     return f'{CHARACTER_NAME} said {input}'

# if __name__ == '__main__':
#     app.run(host=SERVICE_HOST, port=SERVICE_PORT)

from flask import Flask
from flask import request
from flask import make_response
from flask import render_template
import logging
import os
import sys
import psycopg
from psycopg.rows import dict_row

SERVICE_HOST = os.getenv('SERVER_HOST', '0.0.0.0')
SERVICE_PORT = int(os.getenv('SERVER_PORT', 8080))
CHARACTER_NAME = os.getenv('CHARACTER_NAME', 'I')

# 例:
# postgresql://user:password@host:5432/dbname
POSTGRES_URL = os.getenv('POSTGRES_URL')


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)

        handler.setFormatter(
            logging.Formatter(
                '%(name)s [%(asctime)s] [%(levelname)s] %(message)s'
            )
        )

        logger.addHandler(handler)

    return logger


logger = get_logger('echo-service')

app = Flask(__name__)


def get_connection():
    return psycopg.connect(
        POSTGRES_URL,
        row_factory=dict_row
    )


def init_table():
    sql = """
    CREATE TABLE IF NOT EXISTS echo_messages (
        id SERIAL PRIMARY KEY,
        input_text TEXT NOT NULL,
        response_text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


@app.before_request
def startup():
    init_table()


@app.get("/healthcheck")
def readiness_probe():
    return "I'm ready!"


@app.get("/echo")
def get_echo_history():
    sql = """
    SELECT
        id,
        input_text,
        response_text,
        created_at
    FROM echo_messages
    ORDER BY created_at DESC
    LIMIT 100
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    response = make_response({"data": rows})
    response.headers['Content-type'] = 'application/json'

    return response


@app.post("/echo")
def echo():
    '''
    Main handler for input data sent by Snowflake.
    '''

    message = request.json

    logger.debug(f'Received request: {message}')

    if message is None or not message['data']:
        logger.info('Received empty message')
        return {}

    input_rows = message['data']

    output_rows = []

    insert_sql = """
    INSERT INTO echo_messages (
        input_text,
        response_text
    )
    VALUES (%s, %s)
    """

    with get_connection() as conn:
        with conn.cursor() as cur:

            for row in input_rows:
                row_index = row[0]
                input_text = row[1]

                response_text = get_echo_response(input_text)

                cur.execute(
                    insert_sql,
                    (input_text, response_text)
                )

                output_rows.append(
                    [row_index, response_text]
                )

        conn.commit()

    logger.info(f'Produced {len(output_rows)} rows')

    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'

    logger.debug(f'Sending response: {response.json}')

    return response

@app.route("/ui", methods=["GET", "POST"])
def ui():

    input_text = None
    response_text = None

    if request.method == "POST":

        input_text = request.form.get("input")
        response_text = get_echo_response(input_text)

        insert_sql = """
        INSERT INTO echo_messages (
            input_text,
            response_text
        )
        VALUES (%s, %s)
        """

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(insert_sql, (input_text, response_text))
            conn.commit()

    # ★ここはGET/POST両方で必ず実行
    history_sql = """
    SELECT
        input_text,
        response_text,
        created_at
    FROM echo_messages
    ORDER BY created_at DESC
    LIMIT 20
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(history_sql)
            history = cur.fetchall()

    return render_template(
        "basic_ui.html",
        echo_input=input_text,
        echo_reponse=response_text,
        history=history
    )


def get_echo_response(input_text):
    return f'{CHARACTER_NAME} said {input_text}'


if __name__ == '__main__':
    app.run(
        host=SERVICE_HOST,
        port=SERVICE_PORT
    )