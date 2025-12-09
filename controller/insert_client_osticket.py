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
        query = f"INSERT INTO ost_user ({', '.join(cols)}) VALUES ({', '.join(['{}'] * len(cols))})"

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
            "org_id",
            "default_email_id",
            "status",
            "name",
            "created",
            "updated",
        ]

        for k in mapping:
            if k in payload and payload[k] not in (None, ""):
                out[k] = payload[k]

        return out

    def create_user_email(self, data) -> int:
        """Inserta un nuevo email. Devuelve cantidad de filas afectadas (si el conector lo soporta)."""
        # Normalizar a lista de filas
        rows = [data] if isinstance(data, dict) else list(data)
        if not rows:
            return 0

        # Construir columnas y placeholders a partir de la primera fila
        keys = list(rows[0].keys())
        cols = [f"{k}" for k in keys]
        query = f"INSERT INTO ost_user_email ({', '.join(cols)}) VALUES ({', '.join(['{}'] * len(cols))})"

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
            print(f"Error creando email(s): {e}")
            return 0

    # Utility to map form fields to DB columns with basic defaults
    @staticmethod
    def normalize_payload_user_email(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        mapping = [
            "id",
            "user_id",
            "flags",
            "address",
        ]

        for k in mapping:
            if k in payload and payload[k] not in (None, ""):
                out[k] = payload[k]

        return out

    def create_os_form_entry(self, data) -> int:
        """Inserta un nuevo formulario. Devuelve cantidad de filas afectadas (si el conector lo soporta)."""
        # Normalizar a lista de filas
        rows = [data] if isinstance(data, dict) else list(data)
        if not rows:
            return 0

        # Construir columnas y placeholders a partir de la primera fila
        keys = list(rows[0].keys())
        cols = [f"{k}" for k in keys]
        query = f"INSERT INTO ost_form_entry ({', '.join(cols)}) VALUES ({', '.join(['{}'] * len(cols))})"

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
            print(f"Error creando formulario(s): {e}")
            return 0

    # Utility to map form fields to DB columns with basic defaults
    @staticmethod
    def normalize_payload_form_entry(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        mapping = [
            "id",
            "form_id",
            "object_id",
            "object_type",
            "sort",
            "extra",
            "created",
            "updated",
        ]

        for k in mapping:
            if k in payload and payload[k] not in (None, ""):
                out[k] = payload[k]

        return out

    def create_os_form_entry_values(self, data) -> int:
        """Inserta nuevos valores de formulario. Devuelve cantidad de filas afectadas (si el conector lo soporta)."""
        # Normalizar a lista de filas
        rows = [data] if isinstance(data, dict) else list(data)
        if not rows:
            return 0

        # Construir columnas y placeholders a partir de la primera fila
        keys = list(rows[0].keys())
        cols = [f"{k}" for k in keys]
        query = f"INSERT INTO ost_form_entry_values ({', '.join(cols)}) VALUES ({', '.join(['{}'] * len(cols))})"

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
            print(f"Error creando valores de formulario: {e}")
            return 0

    # Utility to map form fields to DB columns with basic defaults
    @staticmethod
    def normalize_payload_form_entry_values(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        mapping = [
            "entry_id",
            "field_id",
            "value",
            "value_id",
        ]

        for k in mapping:
            if k in payload and payload[k] not in (None, ""):
                out[k] = payload[k]

        return out


if __name__ == "__main__":
    import os
    import sys

    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector
    from conn.mysql_connector import MySQLConnector
    from clients.clientes_OSTickect import Clientes

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
    oInsertClientes = InsertClientes(db=db)
    oClientes = Clientes(db=db)
    oClientes.new_cod_cliente()
    new_id_cliente = oClientes.next_cod_cliente()
    oClientes.new_cod_form_entry()
    # print(oClientes.get_clientes_mikrowisp())
    safe1 = []
    safe2 = []
    safe3 = []
    safe4 = []
    payload_cliente1 = {
        "id": new_id_cliente,
        "org_id": 0,
        "name": "PRUEBA PYTHON OSTICKET",
        "default_email_id": new_id_cliente,
        "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    safe1.append(oInsertClientes.normalize_payload_cliente(payload_cliente1))
    rows_count_clientes = oInsertClientes.create_clientes(safe1)
    print(f"filas afectadas: {rows_count_clientes}")
    if rows_count_clientes:
        # Confirmar clientes insertados, ahora se pueden insertar los emails
        db.commit()
        payload_user_mail1 = {
            "id": new_id_cliente,
            "user_id": new_id_cliente,
            "flags": 0,
            "address": "email@example.com",
        }
        safe2.append(oInsertClientes.normalize_payload_user_email(payload_user_mail1))
        rows_count_user_email = oInsertClientes.create_user_email(safe2)
        if rows_count_user_email:
            print(f"filas afectadas (user_email): {rows_count_user_email}")
            db.commit()
            new_id_form_entry = oClientes.next_cod_form_entry()
            payload_form_entry1 = {
                "id": new_id_form_entry,
                "form_id": 1,
                "object_id": new_id_cliente,
                "object_type": "U",
                "sort": 1,
                "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            safe3.append(
                oInsertClientes.normalize_payload_form_entry(payload_form_entry1)
            )
            rows_count_form_entry = oInsertClientes.create_os_form_entry(safe3)
            if rows_count_form_entry:
                print(f"filas afectadas (form_entry): {rows_count_form_entry}")
                db.commit()
                payload_form_entry_value1 = {
                    "entry_id": new_id_form_entry,
                    "field_id": 3,
                    "value": " ",
                }
                payload_form_entry_value2 = {
                    "entry_id": new_id_form_entry,
                    "field_id": 108,
                    "value": "CL000",
                }
                payload_form_entry_value3 = {
                    "entry_id": new_id_form_entry,
                    "field_id": 109,
                    "value": "ADI PRUEBA PYTHON OSTICKET",
                }
                payload_form_entry_value4 = {
                    "entry_id": new_id_form_entry,
                    "field_id": 110,
                    "value": "ACTIVO",
                }

                safe4.append(
                    oInsertClientes.normalize_payload_form_entry_values(
                        payload_form_entry_value1
                    )
                )
                safe4.append(
                    oInsertClientes.normalize_payload_form_entry_values(
                        payload_form_entry_value2
                    )
                )
                safe4.append(
                    oInsertClientes.normalize_payload_form_entry_values(
                        payload_form_entry_value3
                    )
                )
                safe4.append(
                    oInsertClientes.normalize_payload_form_entry_values(
                        payload_form_entry_value4
                    )
                )
                rows_count_form_entry_values = (
                    oInsertClientes.create_os_form_entry_values(safe4)
                )
                if rows_count_form_entry_values:
                    print(
                        f"filas afectadas (form_entry_values): {rows_count_form_entry_values}"
                    )
                    db.commit()
                else:
                    print("No se insertaron valores de formulario.")
                    db.rollback()
            else:
                print("No se insertaron formularios.")
                db.rollback()

        else:
            print("No se insertaron emails.")
            db.rollback()
    else:
        print("No se insertaron clientes.")
        db.rollback()

    db.autocommit(True)
    db.close_connection()
