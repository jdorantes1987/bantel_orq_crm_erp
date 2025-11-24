from pandas import concat

from clients.clientes_crm_ventas import ClientesCRM
from clients.clientes_mikrowisp import Clientes
from clients.clientes_profit import ClientesMonitoreoProfit


class SyncClientes:
    def __init__(self, db_crm, db_mikrowisp, db_profit_fact, db_profit_recibos):
        self.oClientesCRM = ClientesCRM(db_crm)
        self.oClientesProfitFacturas = ClientesMonitoreoProfit(db_profit_fact)
        self.oClientesProfitRecibos = ClientesMonitoreoProfit(db_profit_recibos)
        self.oClientesMikrowisp = Clientes(db_mikrowisp)

    def clientes_x_sincronizar_en_profit(self):
        clientes_crm = self.oClientesCRM.obtener_clientes()
        clientes_profit_facturas = (
            self.oClientesProfitFacturas.obtener_clientes_activos()
        )
        clientes_profit_recibos = self.oClientesProfitRecibos.obtener_clientes_activos()
        clientes_profit = concat(
            [clientes_profit_facturas, clientes_profit_recibos],
            ignore_index=True,
            axis=0,
        )
        # Filtrar clientes de Profit que en un principio no están en CRM
        clientes_profit = clientes_profit[clientes_profit["co_seg"] != "A_CRM"]
        set_cod_crm = set(clientes_crm["codigo_cliente"])
        set_cod_profit = set(clientes_profit["co_cli"])
        codigos_a_agregar = set_cod_crm.difference(set_cod_profit)
        clientes_a_agregar = clientes_crm[
            clientes_crm["codigo_cliente"].isin(codigos_a_agregar)
        ]
        return clientes_a_agregar

    def clientes_x_sincronizar_en_mikrowisp(self):
        clientes_mw = self.oClientesMikrowisp.get_clientes_mikrowisp()
        clientes_profit_facturas = (
            self.oClientesProfitFacturas.obtener_clientes_activos()
        )
        clientes_profit_recibos = self.oClientesProfitRecibos.obtener_clientes_activos()
        clientes_profit = concat(
            [clientes_profit_facturas, clientes_profit_recibos],
            ignore_index=True,
            axis=0,
        )
        set_cod_mw = set(clientes_mw["codigo_cliente"])
        set_cod_profit = set(clientes_profit["co_cli"])
        codigos_a_agregar = set_cod_profit.difference(set_cod_mw)
        clientes_a_agregar = clientes_profit[
            clientes_profit["co_cli"].isin(codigos_a_agregar)
        ]
        return clientes_a_agregar


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.mysql_connector import MySQLConnector
    from conn.sql_server_connector import SQLServerConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para SQL Server
    db_credentials = {
        "host": os.getenv("HOST_PRODUCCION_PROFIT"),
        "database": os.getenv("DB_NAME_DERECHA_PROFIT"),
        "user": os.getenv("DB_USER_PROFIT"),
        "password": os.getenv("DB_PASSWORD_PROFIT"),
    }

    # Conexión a la base de datos de la derecha
    sqlserver_connector_fact = SQLServerConnector(**db_credentials)
    sqlserver_connector_fact.connect()
    db_profit_fact = DatabaseConnector(sqlserver_connector_fact)
    db_profit_fact.autocommit(False)
    db_credentials["database"] = os.getenv("DB_NAME_IZQUIERDA_PROFIT")
    sqlserver_connector_recibos = SQLServerConnector(**db_credentials)
    sqlserver_connector_recibos.connect()
    db_profit_recibos = DatabaseConnector(sqlserver_connector_recibos)
    db_profit_recibos.autocommit(False)

    # Para MySql CRM Ventas
    mysql_connector = MySQLConnector(
        host=os.environ["HOST_PRODUCCION_CRM_VENTAS"],
        database=os.environ["DB_NAME_CRM_VENTAS"],
        user=os.environ["DB_USER_CRM_VENTAS"],
        password=os.environ["DB_PASSWORD_CRM_VENTAS"],
    )
    mysql_connector.connect()
    db_crm = DatabaseConnector(mysql_connector)
    db_crm.autocommit(False)

    # Para MySql Mikrowisp
    mysql_connector = MySQLConnector(
        host=os.environ["HOST_PRODUCCION_MKWSP"],
        database=os.environ["DB_NAME_MKWSP"],
        user=os.environ["DB_USER_MKWSP"],
        password=os.environ["DB_PASSWORD_MKWSP"],
    )
    mysql_connector.connect()
    db_mw = DatabaseConnector(mysql_connector)
    db_mw.autocommit(False)

    oSyncClientes = SyncClientes(
        db_crm=db_crm,
        db_mikrowisp=db_mw,
        db_profit_fact=db_profit_fact,
        db_profit_recibos=db_profit_recibos,
    )
    print(oSyncClientes.clientes_x_sincronizar_en_profit())

    db_profit_fact.autocommit(True)
    db_crm.autocommit(True)
    db_mw.autocommit(True)
    db_profit_fact.close_connection()
    db_profit_recibos.close_connection()
    db_crm.close_connection()
    db_mw.close_connection()
