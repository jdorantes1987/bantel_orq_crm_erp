from typing import Any, Dict

from pandas import read_sql_query


class ClientesCRM:
    def __init__(self, db):
        self.db = db
        self.cursor = self.db.get_cursor()
        self.c_engine = db.conn_engine()

    def obtener_clientes(self):
        """Obtiene todos los clientes de Mikrowisp."""

        query = """
                SELECT account.id,
                        account.codigo_cliente,
                        account.NAME AS name,
                        account.direccion_de_facturacion,
                        account.empresa,
                        account.coordenadas_g_p_s,
                        account.r_i_f,
                        account.cedula,
                        account.created_at,
                        contact.tipo_de_contacto,
                        contact.first_name,
                        contact.last_name,
                        contact.direccion_tecnica,
                        phone_number.numeric AS num_admin,
                        phone_number_1.numeric AS num_tecnico,
                        account.m_pago,
                        email_address.lower AS admin_email,
                        email_address_1.lower AS tenico_email,
                        Concat(op.tipo_de_servicio, ' ', op.capacidad_de_servicio) AS servicio,
                        'Cuenta' AS apartado
                    FROM   ((((((((account
                                INNER JOIN account_contact
                                        ON account.id = account_contact.account_id)
                                INNER JOIN contact
                                        ON ( account_contact.account_id = contact.account_id )
                                            AND ( account_contact.contact_id = contact.id ))
                                LEFT JOIN (entity_phone_number
                                            LEFT JOIN phone_number
                                                ON entity_phone_number.phone_number_id =
                                        phone_number.id)
                                        ON account.id = entity_phone_number.entity_id)
                                INNER JOIN entity_phone_number AS entity_phone_number_1
                                        ON contact.id = entity_phone_number_1.entity_id)
                            INNER JOIN phone_number AS phone_number_1
                                    ON entity_phone_number_1.phone_number_id = phone_number_1.id)
                            INNER JOIN entity_email_address
                                    ON account.id = entity_email_address.entity_id)
                            INNER JOIN email_address
                                    ON entity_email_address.email_address_id = email_address.id)
                            INNER JOIN entity_email_address AS entity_email_address_1
                                    ON contact.id = entity_email_address_1.entity_id)
                        INNER JOIN email_address AS email_address_1
                                ON entity_email_address_1.email_address_id = email_address_1.id
                        LEFT JOIN opportunity AS op
                                ON account.id = op.account_id
                    WHERE  account.deleted = '0'
                    UNION
                    SELECT c2.id,
                        c2.codigo_de_cliente,
                        c2.NAME as name,
                        c2.direccion_de_facturacion,
                        c2.empresa,
                        c2.coordenadas_g_p_s,
                        c2.r_i_f,
                        c2.cedula,
                        c2.created_at,
                        c2.tipo_de_contacto,
                        c2.first_name,
                        c2.last_name,
                        c2.direccion_tecnica,
                        c2.num_admin,
                        c2.num_tecnico,
                        c2.m_pago,
                        c2.admin_email,
                        c2.tenico_email,
                        c2.servicio,
                        'Referido' AS apartado
                    FROM   (SELECT r.id,
                                r.NAME,
                                r.codigo_de_cliente,
                                c1.direccion_de_facturacion,
                                c1.empresa,
                                c1.coordenadas_g_p_s,
                                c1.r_i_f,
                                c1.cedula,
                                c1.created_at,
                                c1.tipo_de_contacto,
                                c1.first_name,
                                c1.last_name,
                                c1.direccion_tecnica,
                                c1.num_admin,
                                c1.num_tecnico,
                                c1.m_pago,
                                c1.admin_email,
                                c1.tenico_email,
                                Concat(r.tipo_de_servicio, ' ', r.capacidad_de_servicio) AS
                                servicio
                            FROM   referido AS r
                                INNER JOIN account_referido AS ar
                                        ON r.id = ar.referido_id
                                LEFT JOIN (SELECT account.id,
                                                    account.direccion_de_facturacion,
                                                    account.empresa,
                                                    account.coordenadas_g_p_s,
                                                    account.r_i_f,
                                                    account.cedula,
                                                    account.created_at,
                                                    contact.tipo_de_contacto,
                                                    contact.first_name,
                                                    contact.last_name,
                                                    contact.direccion_tecnica,
                                                    phone_number.numeric   AS num_admin,
                                                    phone_number_1.numeric AS num_tecnico,
                                                    account.m_pago,
                                                    email_address.lower    AS admin_email,
                                                    email_address_1.lower  AS tenico_email
                                            FROM   ((((((((account
                                                            INNER JOIN account_contact
                                                                    ON account.id =
                                                                        account_contact.account_id)
                                                            INNER JOIN contact
                                                                    ON ( account_contact.account_id
                                                                        =
                                                                        contact.account_id )
                                                                    AND (
                                                            account_contact.contact_id =
                                                            contact.id ))
                                                        LEFT JOIN (entity_phone_number
                                                                    LEFT JOIN phone_number
                                                                            ON
                        entity_phone_number.phone_number_id
                        =
                                                                    phone_number.id)
                                                                ON account.id =
                        entity_phone_number.entity_id)
                                                        INNER JOIN entity_phone_number AS
                                                                    entity_phone_number_1
                                                                ON contact.id =
                        entity_phone_number_1.entity_id)
                                                        INNER JOIN phone_number AS phone_number_1
                                                                ON
                                                        entity_phone_number_1.phone_number_id =
                                                        phone_number_1.id)
                                                        INNER JOIN entity_email_address
                                                                ON account.id =
                        entity_email_address.entity_id)
                                                    INNER JOIN email_address
                                                            ON
                                                    entity_email_address.email_address_id =
                                                    email_address.id)
                                                    INNER JOIN entity_email_address AS
                                                                entity_email_address_1
                                                            ON contact.id =
                        entity_email_address_1.entity_id)
                                                    INNER JOIN email_address AS email_address_1
                                                            ON
                                                    entity_email_address_1.email_address_id =
                                                    email_address_1.id
                                            WHERE  account.deleted = '0') AS c1
                                        ON ar.account_id = c1.id
                            WHERE  r.deleted = 0) AS c2
                """
        return read_sql_query(query, self.c_engine)

    def update_clientes(self, data, entity_cliente: str = "account") -> int:
        """Actualiza uno o varios clientes en CRM Ventas.
        Args:
            data (Dict[str, Any] | List[Dict[str, Any]]): Datos del cliente o lista de clientes a actualizar.
            entity_cliente (str): Tipo de entidad a actualizar ('account' o 'referidos'). Por defecto es 'account'.
        Returns:
            int: Cantidad de filas afectadas.
        """
        # Normalizar a lista de filas
        rows = [data] if isinstance(data, dict) else list(data)
        if not rows:
            return 0

        if entity_cliente == "account":
            # Aplicar normalización básica a cada payload (fechas, campos vacíos, etc.)
            rows = [self.normalize_payload_cliente(r) for r in rows]
        elif entity_cliente == "referido":
            rows = [self.normalize_payload_referido(r) for r in rows]

        # Filtrar filas válidas y preparar columnas a actualizar (excluir id)
        prepared = []  # list of (row_dict, set_columns)
        for r in rows:
            id = r.get("id")
            if not id:
                # omitimos filas sin clave primaria
                continue
            set_cols = [k for k in r.keys() if k != "id"]
            if not set_cols:
                # nada que actualizar
                continue
            prepared.append((r, set_cols))

        if not prepared:
            return 0

        # Agrupar por conjunto de columnas a actualizar para poder usar executemany
        groups = {}
        for r, set_cols in prepared:
            key = tuple(set_cols)
            groups.setdefault(key, []).append(r)

        total_affected = 0
        try:
            for key, group_rows in groups.items():
                set_cols = list(key)
                set_clause = ", ".join([f"{c} = {{}}" for c in set_cols])
                query = f"UPDATE {entity_cliente} SET {set_clause} WHERE id = {{}}"

                params = [
                    tuple(r.get(c) for c in set_cols) + (r.get("id"),)
                    for r in group_rows
                ]

                self.db.executemany(query, params)

                rc = getattr(self.db, "rowcount", -1)
                if rc is None or rc < 0:
                    total_affected += len(params)
                else:
                    total_affected += rc

            return total_affected
        except Exception as e:
            print(f"Error actualizando cliente(s): {e}")
            return 0

    # Utility to map form fields to DB columns with basic defaults
    @staticmethod
    def normalize_payload_cliente(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        mapping = [
            "id",
            "name",
            "deleted",
            "website",
            "type",
            "industry",
            "sic_code",
            "billing_address_street",
            "billing_address_city",
            "billing_address_state",
            "billing_address_country",
            "billing_address_postal_code",
            "shipping_address_street",
            "shipping_address_city",
            "shipping_address_state",
            "shipping_address_country",
            "shipping_address_postal_code",
            "description",
            "created_at",
            "modified_at",
            "campaign_id",
            "created_by_id",
            "modified_by_id",
            "assigned_user_id",
            "shipping_address_avenue",
            "shipping_address_full",
            "m_pago",
            "address_street",
            "address_city",
            "address_state",
            "address_country",
            "address_postal_code",
            "addressfact_street",
            "addressfact_city",
            "addressfact_state",
            "addressfact_country",
            "addressfact_postal_code",
            "copyaddress_street",
            "copyaddress_city",
            "copyaddress_state",
            "copyaddress_country",
            "copyaddress_postal_code",
            "billing_street",
            "billing_city",
            "billing_state",
            "billing_country",
            "billing_postal_code",
            "direccion_tecnica",
            "direccion_de_facturacion",
            "coordenadas_g_p_s",
            "first_name",
            "middle_name",
            "last_name",
            "tipo_de_contacto",
            "empresa",
            "lead_id",
            "opportunity_id",
            "r_i_f",
            "cedula",
            "codigo_cliente",
            "rifdoc_id",
            "ceduladoc_id",
            "m_pago",
            "account_parent_id",
        ]

        for k in mapping:
            if k in payload and payload[k] not in (None, ""):
                out[k] = payload[k]

        return out

    # Utility to map form fields to DB columns with basic defaults
    @staticmethod
    def normalize_payload_referido(payload: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        mapping = [
            "id",
            "name",
            "deleted",
            "description",
            "created_at",
            "modified_at",
            "created_by_id",
            "modified_by_id",
            "assigned_user_id",
            "codigo_de_cliente",
            "tipo_de_servicio",
            "capacidad_de_servicio",
            "ubicacion",
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

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Para MySql
    mysql_connector = MySQLConnector(
        host=os.environ["HOST_PRODUCCION_CRM_VENTAS"],
        database=os.environ["DB_NAME_CRM_VENTAS"],
        user=os.environ["DB_USER_CRM_VENTAS"],
        password=os.environ["DB_PASSWORD_CRM_VENTAS"],
    )
    mysql_connector.connect()
    db = DatabaseConnector(mysql_connector)
    db.autocommit(False)
    oClientesCRM = ClientesCRM(db=db)
    print(oClientesCRM.obtener_clientes())
    # oClientesCRM.update_clientes(
    #     {
    #         "id": "6924aaf28d554fbb8",
    #         "r_i_f": " ",
    #     }
    # )
    # print("Cliente(s) actualizado(s).")
    db.autocommit(True)
    db.close_connection()
