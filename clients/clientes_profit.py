import sys
from datetime import datetime
from pandas import read_sql_query

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

    # Genera el siguiente código de cliente
    def next_cod_cliente(self) -> str:
        return "CL" + self._set_numero_cliente()

    # Obtiene el último número de cliente usado y lo guarda internamente
    def new_cod_cliente(self):
        sql = """
                SELECT MAX(TRY_CAST(
                    CASE
                        WHEN PATINDEX('%[^0-9]%', SUBSTRING(co_cli, PATINDEX('%[0-9]%', co_cli), LEN(co_cli))) > 0
                        THEN LEFT(
                                SUBSTRING(co_cli, PATINDEX('%[0-9]%', co_cli), LEN(co_cli)),
                                PATINDEX('%[^0-9]%', SUBSTRING(co_cli, PATINDEX('%[0-9]%', co_cli), LEN(co_cli))) - 1
                            )
                        ELSE SUBSTRING(co_cli, PATINDEX('%[0-9]%', co_cli), LEN(co_cli))
                    END
                AS INT)) as num_co_cli
                FROM saCliente
            """
        cur = self.db.execute(sql, ())
        num_max = cur.fetchone()
        self._last_num_cliente = num_max[0] if num_max else 0

    def get_clients_inserted_today(self):
        """Obtiene los clientes agregados hoy."""
        hoy = datetime.now().strftime("%Y-%m-%d")
        sql = """select RTRIM(co_cli) as co_cli, cli_des, fe_us_in, co_us_in
                 from saCliente
                 where CAST(fe_us_in AS DATE) = {}
            """
        result = read_sql_query(sql.format(f"'{hoy}'"), self.c_engine)
        return result


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
    oClientesMonitoreo.new_cod_cliente()
    new_cod_cliente = oClientesMonitoreo.next_cod_cliente()
    print(new_cod_cliente)
    new_cod_cliente = oClientesMonitoreo.next_cod_cliente()
    print(new_cod_cliente)
    # clientes_hoy = oClientesMonitoreo.get_clients_inserted_today()
    # print(clientes_hoy)
    db_derecha.close_connection()
