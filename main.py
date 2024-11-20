from my_lib.analyze import analyze
from my_lib.read import connect_db
from my_lib.transformation import transform

if __name__ == "__main__":
    conn = connect_db()
    if conn:
        transform(conn)
        analyze(conn)
        conn.close()