from typing import Any

from pandas import read_sql_query


class Clientes:
    def __init__(self, db):
        self.db = db
        self.cursor = self.db.get_cursor()
        self.c_engine = db.conn_engine()

    def get_clientes_mikrowisp(self):
        """Obtiene todos los clientes de Mikrowisp."""
        query = "SELECT * FROM usuarios"
        return read_sql_query(query, self.c_engine)

    def get_id_cliente_by_codigo(self, codigo_cliente: str) -> Any:
        """Obtiene el ID del cliente por su código."""
        sql = "SELECT id FROM usuarios WHERE codigo_cliente = {}"
        cur = self.db.execute(sql, (codigo_cliente,))
        row = cur.fetchone()
        return row["id"] if row else None


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.mysql_connector import MySQLConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para MySql
    mysql_connector = MySQLConnector(
        host=os.environ["HOST_PRODUCCION_MKWSP"],
        database=os.environ["DB_NAME_MKWSP"],
        user=os.environ["DB_USER_MKWSP"],
        password=os.environ["DB_PASSWORD_MKWSP"],
    )
    mysql_connector.connect()
    db = DatabaseConnector(mysql_connector)
    db.autocommit(False)
    oClientes = Clientes(db=db)
    print(oClientes.get_clientes_mikrowisp())
