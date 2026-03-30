import psycopg2


def get_connection(host, port, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )

        return conn
    except psycopg2.Error as e:
        print(e)
        return None
