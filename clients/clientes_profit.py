import sys

sys.path.append("../profit")
from data.mod.ventas.clientes import Clientes  # noqa: E402


class ClientesMonitoreoProfit:
    def __init__(self, db):
        self.db = db
        self.cursor = self.db.get_cursor()
        self.c_engine = db.conn_engine()
        self.oClientes = Clientes(db)
        self.counter_num_cliente = 0
        self._last_num_cliente = 0

    def obtener_clientes_activos(self):
        """Obtiene una lista de clientes activos."""
        data = self.oClientes.get_clientes_profit()
        return data[data["inactivo"] == 0]

    def _set_numero_cliente(self):
        self._last_num_cliente += 1
        return str(self._last_num_cliente).zfill(0)

    def next_cod_cliente(self) -> str:
        return "CL" + self._set_numero_cliente()

    def get_new_cod_cliente(self) -> str:
        sql = """select MAX(TRY_PARSE(SUBSTRING(co_cli,PATINDEX('%[0-9]%',co_cli),LEN(co_cli)) AS INT)) as num_co_cli
                 from saCliente
                 where tipo_adi=1
            """
        cur = self.db.execute(sql, ())
        num_max = cur.fetchone()
        self._last_num_cliente = num_max[0] if num_max else 0
        return self.next_cod_cliente()


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # diccionario con las credenciales de la base de datos
    # para manejar multiples conexiones a la vez

    db_credentials = {
        "host": os.getenv("HOST_PRODUCCION_PROFIT"),
        "database": os.getenv("DB_NAME_IZQUIERDA_PROFIT"),
        "user": os.getenv("DB_USER_PROFIT"),
        "password": os.getenv("DB_PASSWORD_PROFIT"),
    }

    # Conexión a la base de datos de la derecha
    sqlserver_connector = SQLServerConnector(**db_credentials)
    sqlserver_connector.connect()
    db_derecha = DatabaseConnector(sqlserver_connector)
    oClientesMonitoreo = ClientesMonitoreoProfit(db_derecha)
    last_number = oClientesMonitoreo.get_new_cod_cliente()
    print(last_number)
    new_cod_cliente = oClientesMonitoreo.next_cod_cliente()
    print(new_cod_cliente)
    new_cod_cliente = oClientesMonitoreo.next_cod_cliente()
    print(new_cod_cliente)
    db_derecha.close_connection()
