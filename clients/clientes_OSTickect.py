class Clientes:
    def __init__(self, db):
        self.db = db
        self.cursor = self.db.get_cursor()
        self.c_engine = db.conn_engine()
        self.counter_num_cliente = 0
        self._last_num_cliente = 0
        self.counter_num_form_entry = 0
        self._last_num_form_entry = 0

    def new_cod_cliente(self):
        """Genera un nuevo código de cliente."""
        sql = "SELECT max(id) as id FROM ost_user"
        cur = self.db.execute(sql, ())
        row = cur.fetchone()
        self._last_num_cliente = row["id"]

    def _set_numero_cliente(self):
        self._last_num_cliente += 1
        return str(self._last_num_cliente).zfill(0)

    # Genera el siguiente código de cliente
    def next_cod_cliente(self) -> str:
        return self._set_numero_cliente()

    def new_cod_form_entry(self):
        """Genera un nuevo código de formulario."""
        sql = "SELECT max(id) as id FROM ost_form_entry"
        cur = self.db.execute(sql, ())
        row = cur.fetchone()
        self._last_num_form_entry = row["id"]

    def _set_numero_form_entry(self):
        self._last_num_form_entry += 1
        return str(self._last_num_form_entry).zfill(0)

    # Genera el siguiente código de formulario
    def next_cod_form_entry(self) -> str:
        return self._set_numero_form_entry()


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
        host=os.environ["HOST_DEV_OSTICKET"],
        database=os.environ["DB_NAME_OSTICKET"],
        user=os.environ["DB_USER_OSTICKET"],
        password=os.environ["DB_PASSWORD_OSTICKET"],
    )
    mysql_connector.connect()
    db = DatabaseConnector(mysql_connector)
    db.autocommit(False)
    oClientes = Clientes(db=db)
    oClientes.new_cod_form_entry()
    print(oClientes.next_cod_form_entry())
    print(oClientes.next_cod_form_entry())
    db.close_connection()
