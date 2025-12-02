import os
import sys

from time import sleep
import streamlit as st
from dotenv import load_dotenv

from clients.clientes_mikrowisp import Clientes
from clients.clientes_profit import ClientesMonitoreoProfit
from controller.sync_clientes import SyncClientes

sys.path.append("../authenticator")
sys.path.append("../profit")
sys.path.append("../conexiones")

from auth import AuthManager  # noqa: E402
from conn.database_connector import DatabaseConnector  # noqa: E402
from conn.mysql_connector import MySQLConnector  # noqa: E402
from conn.sql_server_connector import SQLServerConnector  # noqa: E402
from data.mod.ventas.tabuladorISLR import TabuladorISLR  # noqa: E402
from role_manager_db import RoleManagerDB  # noqa: E402

# Configuración de página con fondo personalizado
st.set_page_config(
    page_title="Login",
    layout="centered",
    initial_sidebar_state="collapsed",
    page_icon="",
)

MENU_INICIO = "pages/page2.py"

st.title("Inicio de sesión")


def abrir_conexion_db():
    # Abrir conexiones
    st.session_state.conexion_facturas._connector.connect()
    st.session_state.conexion_recibos._connector.connect()
    st.session_state.conexion_crm._connector.connect()
    st.session_state.conexion_mw._connector.connect()

    # Ajustar autocommit a False para permitir transacciones
    st.session_state.conexion_facturas.autocommit(False)
    st.session_state.conexion_recibos.autocommit(False)
    st.session_state.conexion_crm.autocommit(False)
    st.session_state.conexion_mw.autocommit(False)


def cerrar_conexion_db():
    # Cerrar conexiones
    st.session_state.conexion_facturas.close_connection()
    st.session_state.conexion_recibos.close_connection()
    st.session_state.conexion_crm.close_connection()
    st.session_state.conexion_mw.close_connection()


# Cargar las claves de session si no existen
for key, default in [
    ("stage", 0),
    ("conexion", None),
    ("auth_manager", None),
    ("role_manager", None),
    ("logged_in", False),
    ("user", ""),
    ("cod_client", None),
    ("open_cnn_db", abrir_conexion_db),
    ("close_cnn_db", cerrar_conexion_db),
]:
    if key not in st.session_state:
        st.session_state[key] = default


def set_stage(i):
    st.session_state.stage = i


if st.session_state.stage == 0:
    st.session_state.password = ""
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
    sqlserver_connector_fact = SQLServerConnector(**db_credentials)
    try:
        # Conexión a la base de datos de la derecha
        st.session_state.conexion_facturas = DatabaseConnector(sqlserver_connector_fact)

        # Conexión a la base de datos de la izquierda
        db_credentials["database"] = os.getenv("DB_NAME_IZQUIERDA_PROFIT")
        sqlserver_connector_recibos = SQLServerConnector(**db_credentials)
        st.session_state.conexion_recibos = DatabaseConnector(
            sqlserver_connector_recibos
        )

        # Conexión a MySql CRM Ventas
        mysql_connector = MySQLConnector(
            host=os.environ["HOST_PRODUCCION_CRM_VENTAS"],
            database=os.environ["DB_NAME_CRM_VENTAS"],
            user=os.environ["DB_USER_CRM_VENTAS"],
            password=os.environ["DB_PASSWORD_CRM_VENTAS"],
        )
        st.session_state.conexion_crm = DatabaseConnector(mysql_connector)

        # Conexión a MySql Mikrowisp
        mysql_connector = MySQLConnector(
            host=os.environ["HOST_PRODUCCION_MKWSP"],
            database=os.environ["DB_NAME_MKWSP"],
            user=os.environ["DB_USER_MKWSP"],
            password=os.environ["DB_PASSWORD_MKWSP"],
        )

        st.session_state.conexion_mw = DatabaseConnector(mysql_connector)

        # Almacenar el gestor de autenticación en session_state
        st.session_state.auth_manager = AuthManager(st.session_state.conexion_facturas)
        st.session_state.role_manager = RoleManagerDB(
            st.session_state.conexion_facturas
        )

        # Para poblar estas listas debo abrir la conexión a la base de datos

        # Abrir la conexión a la base de datos
        st.session_state.open_cnn_db()

        # Instanciar TabuladorISLR
        oTab = TabuladorISLR(st.session_state.conexion_facturas)
        tab = oTab.get_tabulador()
        if tab:
            # Obtener solo las claves co_tab y tab_des
            keys = [(d["co_tab"], d["tab_des"]) for d in tab] if tab else []
            # separar por comas los valores de cada tupla
            st.session_state.lista_tab = [" | ".join(map(str, t)) for t in keys]

        # Crear lista de tipos de persona
        st.session_state.list_tipo_persona = [
            "1 | Natural Residente",
            "2 | Natural No Residente",
            "3 | Juridica Domiciliada",
            "4 | Juridica No Domiciliada",
            "5 | Exenta",
            "6 | Tesoreria Nacional",
            "7 | Otros",
            "8 | Otros 2 (fijo=TPE)",
        ]

        # Crear lista de clasificacion de clientes
        st.session_state.list_clasificacion = [
            "1 | Normal",
            "2 | Revendedor",
            "3 | Referido",
        ]

        # Instanciar SyncClientes
        st.session_state.o_sync_clientes = SyncClientes(
            db_crm=st.session_state.conexion_crm,
            db_mikrowisp=st.session_state.conexion_mw,
            db_profit_fact=st.session_state.conexion_facturas,
            db_profit_recibos=st.session_state.conexion_recibos,
        )

        st.session_state.o_clientes_monitoreo_izquierda = ClientesMonitoreoProfit(
            db=st.session_state.conexion_recibos
        )

        st.session_state.o_clientes_monitoreo_derecha = ClientesMonitoreoProfit(
            db=st.session_state.conexion_facturas
        )

        st.session_state.o_clientes_MW = Clientes(db=st.session_state.conexion_mw)

        # Cerrar la conexión a la base de datos
        st.session_state.close_cnn_db()

    except Exception as e:
        st.error(f"No se pudo conectar a la base de datos: {e}")
        st.stop()

    set_stage(1)


def existe_user(username):
    return st.session_state.auth_manager.user_existe(username)


def login(user, passw):
    return st.session_state.auth_manager.autenticar(user, passw)


def iniciar_sesion(user, password):
    # Abrir conexión a la base de datos
    st.session_state.open_cnn_db()
    flag, msg = login(user=user, passw=password)

    if not flag:
        # Si las credenciales son incorrectas
        st.toast(msg, icon="⚠️")
        st.session_state.close_cnn_db()
    else:
        # Verificar permisos
        if st.session_state.rol_user.has_permission("Clientes", "create"):
            st.toast(msg, icon="✅")
            st.session_state.logged_in = True
            st.session_state.user = user
            st.session_state.close_cnn_db()
            st.switch_page(MENU_INICIO)
        else:
            st.error("No tienes permisos para acceder a esta aplicación.")
            del st.session_state.usuario
            sleep(0.4)
            st.session_state.logged_in = False
            set_stage(0)
            st.rerun()


if st.session_state.stage == 1:
    if "usuario" not in st.session_state:
        # Si el usuario aún no ha sido ingresado
        user = st.text_input(
            "", placeholder="Ingresa tu usuario y presiona Enter"
        ).upper()
        if user:
            # Verificar si ingresó un usuario para abrir la conexión a la base de datos
            st.session_state.open_cnn_db()
            if existe_user(user):
                st.session_state.usuario = user
                st.success("Usuario validado!")
                st.session_state.rol_user = (
                    st.session_state.role_manager.load_user_by_username(user)
                )
                st.session_state.close_cnn_db()
                st.rerun()
        else:
            if user:
                st.error("El usuario no existe. Inténtalo de nuevo.")
    else:
        # Si el usuario ya ha sido ingresado, se oculta el input y se muestra el usuario ingresado
        st.write(f"### Usuario ingresado: :blue[{st.session_state.usuario}]")

        # Pedir la contraseña
        pw = st.text_input(
            "",
            type="password",
            key="password",
            placeholder="Ingresa tu contraseña y presiona Enter",
            max_chars=70,
        )
        if st.session_state.password:
            iniciar_sesion(st.session_state.usuario, st.session_state.password)

        if not st.session_state.logged_in:
            if st.button("Atrás"):
                del st.session_state.usuario
                del st.session_state.password
                st.session_state.stage = 0
                st.session_state.stage2 = 0
                st.rerun()

# Cerrar conexiones
# st.session_state.conexion_facturas.close_connection()
# st.session_state.conexion_recibos.close_connection()
# st.session_state.conexion_crm.close_connection()
# st.session_state.conexion_mw.close_connection()
# st.switch_page(MENU_INICIO)
