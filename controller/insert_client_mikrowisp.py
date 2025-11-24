from datetime import datetime
from typing import Any, Dict


class InsertClientes:
    def __init__(self, db):
        self.db = db
        self.cursor = self.db.get_cursor()
        self.c_engine = db.conn_engine()

    def create_clientes(self, data) -> int:
        """Inserta un nuevo cliente. Devuelve cantidad de filas afectadas (si el conector lo soporta)."""
        # Normalizar a lista de filas
        rows = [data] if isinstance(data, dict) else list(data)
        if not rows:
            return 0

        # Construir columnas y placeholders a partir de la primera fila
        keys = list(rows[0].keys())
        cols = [f"{k}" for k in keys]
        query = f"INSERT INTO usuarios ({', '.join(cols)}) VALUES ({', '.join(['{}'] * len(cols))})"

        # Preparar parámetros en el mismo orden de keys para cada fila
        params = [tuple(r.get(k) for k in keys) for r in rows]

        try:
            self.db.executemany(query, params)
            # Algunos conectores no devuelven rowcount confiable después de executemany;
            # si es -1/None devolvemos la cantidad de parámetros enviados.
            rc = getattr(self.db, "rowcount", -1)
            if rc is None or rc < 0:
                return len(params)
            return rc
        except Exception as e:
            print(f"Error creando cliente(s): {e}")
            return 0

    # Utility to map form fields to DB columns with basic defaults
    @staticmethod
    def normalize_payload_cliente(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        mapping = [
            "id",
            "nombre",
            "estado",
            "correo",
            "telefono",
            "movil",
            "cedula",
            "pasarela",
            "codigo",
            "direccion_principal",
            "codigo_cliente",
        ]

        for k in mapping:
            if k in payload and payload[k] not in (None, ""):
                out[k] = payload[k]

        return out

    def create_notificaciones(self, data) -> int:
        """Inserta una nueva notificación. Devuelve cantidad de filas afectadas (si el conector lo soporta)."""
        # Normalizar a lista de filas
        rows = [data] if isinstance(data, dict) else list(data)
        if not rows:
            return 0

        # Construir columnas y placeholders a partir de la primera fila
        keys = list(rows[0].keys())
        cols = [f"{k}" for k in keys]
        query = f"INSERT INTO tblavisouser ({', '.join(cols)}) VALUES ({', '.join(['{}'] * len(cols))})"

        # Preparar parámetros en el mismo orden de keys para cada fila
        params = [tuple(r.get(k) for k in keys) for r in rows]

        try:
            self.db.executemany(query, params)
            # Algunos conectores no devuelven rowcount confiable después de executemany;
            # si es -1/None devolvemos la cantidad de parámetros enviados.
            rc = getattr(self.db, "rowcount", -1)
            if rc is None or rc < 0:
                return len(params)
            return rc
        except Exception as e:
            print(f"Error creando cliente(s): {e}")
            return 0

    # Utility to map form fields to DB columns with basic defaults
    @staticmethod
    def normalize_payload_notificaciones(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        mapping = [
            "id",
            "cliente",
            "mora",
            "reconexion",
            "impuesto",
            "avatar_cliente",
            "chat",
            "zona",
            "diapago",
            "tipopago",
            "tipoaviso",
            "meses",
            "fecha_factura",
            "diafactura",
            "avisopantalla",
            "corteautomatico",
            "avisosms",
            "avisosms2",
            "avisosms3",
            "afip_condicion_iva",
            "afip",
            "afip_condicion_venta",
            "afip_automatico",
            "avatar_color",
            "tiporecordatorio",
            "afip_punto_venta",
            "id_telegram",
            "router_eliminado",
            "otros_impuestos",
            "mikrowisp_app_id",
            "isaviable",
            "invoice_electronic",
            "invoice_data",
            "fecha_suspendido",
            "limit_velocidad",
            "mantenimiento",
            "data_retirado",
            "fecha_retirado",
            "tipo_estrato",
            "fecha_corte_fija",
            "mensaje_comprobante",
            "id_moneda",
            "afip_enable_percepcion",
            "gatewaynoty",
            "fecha_registro",
            "empresa_afip",
            "code_toku",
        ]

        for k in mapping:
            if k in payload and payload[k] not in (None, ""):
                out[k] = payload[k]

        return out


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    from data.clients.clientes_mikrowisp import Clientes

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
    oInsertClientes = InsertClientes(db=db)
    oClientes = Clientes(db=db)
    # print(oClientes.get_clientes_mikrowisp())
    safe1 = []
    safe2 = []
    payload_cliente1 = {
        # "id": "",
        "nombre": "PRUEBA",
        "estado": "ACTIVO",
        "correo": "prueba@ejemplo.com",
        "telefono": "123456789",
        "movil": "987654321",
        "cedula": "12345678",
        "pasarela": "s/data",
        "codigo": "123",
        "direccion_principal": "Calle Falsa 123",
        "codigo_cliente": "CL999",
    }
    safe1.append(oInsertClientes.normalize_payload_cliente(payload_cliente1))
    payload_cliente2 = {
        "nombre": "PRUEBA2",
        "estado": "ACTIVO",
        "correo": "prueba@ejemplo.com",
        "telefono": "123456789",
        "movil": "987654321",
        "cedula": "12345678",
        "pasarela": "s/data",
        "codigo": "123",
        "direccion_principal": "Calle Falsa 123",
        "codigo_cliente": "CL1000",
    }
    safe1.append(oInsertClientes.normalize_payload_cliente(payload_cliente2))
    rows_count_clientes = oInsertClientes.create_clientes(safe1)
    print(f"filas afectadas: {rows_count_clientes}")
    if rows_count_clientes:
        # Confirmar clientes insertados, ahora se pueden insertar las notificaciones
        db.commit()
        # Recorrer los clientes insertados para crear notificaciones
        for cliente in safe1:
            # Obtener el ID de la lista de clientes recién insertados
            cliente_id = oClientes.get_id_cliente_by_codigo(cliente["codigo_cliente"])
            print(f"ID del cliente {cliente['codigo_cliente']}: {cliente_id}")

            payload_notificaciones = {
                "cliente": cliente_id,
                "impuesto": "NADA",
                "chat": 0,
                "zona": 1,
                "diapago": 0,
                "tipopago": 1,
                "tipoaviso": 0,
                "meses": 0,
                "diafactura": 0,
                "avisopantalla": 0,
                "corteautomatico": 0,
                "avisosms": 0,
                "avisosms2": 0,
                "avisosms3": 0,
                "afip_condicion_iva": "Consumidor Final",
                "afip_condicion_venta": "Contado",
                "afip_automatico": 0,
                "avatar_color": "#04CF98",
                "tiporecordatorio": 0,
                "id_telegram": 0,
                "router_eliminado": 0,
                "otros_impuestos": 'a:3:{i:1;s:0:"";i:2;s:0:"";i:3;s:0:"";}',
                "isaviable": 0,
                "invoice_electronic": 0,
                "fecha_suspendido": 0,
                "limit_velocidad": 0,
                "mantenimiento": 0,
                "tipo_estrato": 1,
                "mensaje_comprobante": 0,
                "id_moneda": 1,
                "afip_enable_percepcion": 0,
                "fecha_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "empresa_afip": 1,
            }
            safe2.append(
                oInsertClientes.normalize_payload_notificaciones(payload_notificaciones)
            )

        rows_count_notificaciones = oInsertClientes.create_notificaciones(safe2)
        if rows_count_notificaciones:
            print(f"filas afectadas en notificaciones: {rows_count_notificaciones}")
            db.commit()
        else:
            print("No se insertaron notificaciones, se revierte la operación.")
            db.rollback()

    db.autocommit(True)
    db.close_connection()
