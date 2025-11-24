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
                    account.name,
                    account.direccion_de_facturacion,
                    account.empresa,
                    account.coordenadas_g_p_s,
                    account.r_i_f,
                    account.cedula,
                    contact.tipo_de_contacto,
                    contact.first_name,
                    contact.last_name,
                    contact.direccion_tecnica,
                    phone_number.NUMERIC   AS num_admin,
                    phone_number_1.NUMERIC AS num_tecnico,
                    account.m_pago,
                    email_address.lower    AS admin_email,
                    email_address_1.lower  AS tenico_email
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
                            ON entity_email_address_1.email_address_id = email_address_1.id;
                """
        return read_sql_query(query, self.c_engine)


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
