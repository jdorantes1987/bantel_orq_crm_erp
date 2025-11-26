import time
from datetime import datetime

import streamlit as st
from data.mod.ventas.clientes import Clientes
from clients.clientes_crm_ventas import ClientesCRM
from numpy import where

# Configuración de página con fondo personalizado
st.set_page_config(
    page_title="Clientes por agregar",
    layout="wide",
    initial_sidebar_state="collapsed",
    page_icon="",
)

# Cargar las claves de session si no existen
for key, default in [
    ("stage2", 0),
    ("clientes_para_profit", None),
    ("editor", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


def add_clientes_en_profit(data):
    clientes_count_rows = 0
    # Procesa los clientes seleccionados: separar por módulo y hacer el envío
    # Nota: no usar caché aquí porque la función realiza efectos secundarios
    derecha = data[(data["sel"]) & (data["m_pago"] == "Factura")].copy()
    oClienteCRM = ClientesCRM(db=st.session_state.conexion_crm)
    if not derecha.empty:
        safe1 = []
        updates = []
        oClientesDerecha = Clientes(db=st.session_state.conexion_facturas)
        for index, row in derecha.iterrows():
            payload_cliente = {
                "co_cli": row["r_i_f"],
                "tip_cli": "01",
                "cli_des": row["empresa"] if row["empresa"] else row["name"],
                "inactivo": 0,
                "fecha_reg": datetime.now(),
                "direc1": row["direccion_de_facturacion"],
                "telefonos": row["num_admin"],
                "email": row["admin_email"],
                "puntaje": 0,
                "mont_cre": 0,
                "cond_pag": "01",
                "plaz_pag": 0,
                "desc_ppago": 0,
                "co_zon": "011",
                "co_seg": "ADM",
                "co_ven": "0001",
                "tipo_per": row["tip_persona"].split("|")[0],
                "co_tab": row["tabulador_islr"].split("|")[0],
                "desc_glob": 0,
                "lunes": 0,
                "martes": 0,
                "miercoles": 0,
                "jueves": 0,
                "viernes": 0,
                "sabado": 0,
                "domingo": 0,
                "contrib": 1,
                "co_cta_ingr_egr": "001",
                "juridico": row["es_juridico"],
                "tipo_adi": row["clasif"].split("|")[0],
                "valido": 0,
                "sincredito": 0,
                "contribu_e": row["es_contrib"],
                "rete_regis_doc": 0,
                "porc_esp": 100,
                "Id": 0,
                "co_us_in": "JACK",
                "co_sucu_in": "01",
                "co_us_mo": "JACK",
                "co_sucu_mo": "01",
            }
            safe1.append(oClientesDerecha.normalize_payload_cliente(payload_cliente))
            st.toast(f"Cliente agregado:{row['r_i_f']} - {row['empresa']}", icon="✅")
            item = {
                "id": row["id"],
                "codigo_cliente": row["r_i_f"],
            }
            updates.append(item)

        clientes_count_rows = oClientesDerecha.create_clientes(safe1)
        if clientes_count_rows:
            st.session_state.conexion_facturas.commit()
            oClienteCRM.update_clientes(updates)
            st.session_state.conexion_crm.commit()
        else:
            st.session_state.conexion_facturas.rollback()

    izquierda = data[(data["sel"]) & (data["m_pago"] == "Recibo")].copy()
    # Aquí iría la lógica real de inserción en Profit
    return {
        "derecha_count": clientes_count_rows,
        "izquierda_count": len(izquierda),
    }


def set_stage(i):
    st.session_state.stage2 = i


"""
## Clientes por agregar
"""
if "o_sync_clientes" not in st.session_state:
    st.error("No se ha inicializado la sincronización de clientes.")
    st.stop()

if st.session_state.stage2 == 0:
    clientes = st.session_state.o_sync_clientes.clientes_x_sincronizar_en_profit()
    # Inserta columna llamada 'Select'
    clientes.insert(0, "sel", False)

    # Inserta columna llamada 'tip_persona'
    clientes.insert(1, "tip_persona", st.session_state.list_tipo_persona[0])

    # Inserta columna llamada 'es_juridico'
    clientes.insert(2, "es_juridico", False)

    # Inserta columna llamada 'es_contrib'
    clientes.insert(3, "es_contrib", False)

    # Inserta columna llamada 'tabulador_islr'
    clientes.insert(4, "tabulador_islr", st.session_state.lista_tab[0])

    # Inserta columna llamada 'clasif'
    clientes.insert(5, "clasif", st.session_state.list_clasificacion[0])

    # Guarda en session state
    st.session_state.clientes_para_profit = clientes
    set_stage(1)


if not st.session_state.clientes_para_profit.empty:
    st.info("Tienes clientes por agregar!")
    # Reemplazar valores NaN por cadenas vacías
    st.session_state.clientes_para_profit.fillna("", inplace=True)

    # Asignar tipo de persona Si es o no cliente jurídico
    st.session_state.clientes_para_profit["es_juridico"] = where(
        st.session_state.clientes_para_profit["r_i_f"].str[0] == "J",
        1,
        0,
    )

    # Asignar tipo de persona según la letra inicial del RIF
    st.session_state.clientes_para_profit["tip_persona"] = where(
        st.session_state.clientes_para_profit["r_i_f"].str[0] == "J",
        str(st.session_state.list_tipo_persona[2]),
        str(st.session_state.list_tipo_persona[0]),
    )

    # Asignar tabulador ISLR según la letra inicial del RIF
    st.session_state.clientes_para_profit["tabulador_islr"] = where(
        st.session_state.clientes_para_profit["r_i_f"].str[0] == "J",
        str(st.session_state.lista_tab[2]),
        str(st.session_state.lista_tab[0]),
    )


editor = st.data_editor(
    st.session_state.clientes_para_profit,
    column_config={
        "sel": st.column_config.CheckboxColumn(
            "selec.",
            help="Selecciona el cliente que deseas agregar.",
            width="small",
        ),
        "m_pago": st.column_config.TextColumn(
            "Módulo",
            width="small",
        ),
        "codigo_cliente": st.column_config.TextColumn(
            "Cód. Cliente",
            width="small",
        ),
        "num_admin": st.column_config.TextColumn(
            "Núm. Admin.",
            width="small",
        ),
        "admin_email": st.column_config.TextColumn(
            "Email Admin.",
            width="medium",
        ),
        "es_contrib": st.column_config.CheckboxColumn(
            "Es Contribuyente especial",
            help="Hacer check si el cliente es un contribuyente especial.",
            width="small",
        ),
        "clasif": st.column_config.SelectboxColumn(
            "Clasif.",
            options=st.session_state.list_clasificacion,
            help="Seleccione la clasificación del cliente.",
            width="small",
        ),
        "tip_persona": st.column_config.SelectboxColumn(
            "Tipo Persona",
            options=st.session_state.list_tipo_persona,
            help="Seleccione el tipo de persona del cliente.",
            width="medium",
        ),
        "es_juridico": st.column_config.CheckboxColumn(
            "Es Jurídico",
            help="Hacer check si el cliente es una persona jurídica.",
            width="small",
        ),
        "tabulador_islr": st.column_config.SelectboxColumn(
            "Tabulador ISLR",
            options=(
                st.session_state.lista_tab if "lista_tab" in st.session_state else []
            ),
            help="Selecciona el tabulador de ISLR para el cliente.",
            width="large",
        ),
        "empresa": st.column_config.TextColumn(
            "Empresa",
            width="medium",
        ),
        "created_at": st.column_config.DateColumn(
            "Fecha Creación",
            help="Fecha de creación del cliente.",
            format="DD/MM/YYYY",
        ),
    },
    hide_index=True,
    column_order=[
        "sel",
        "m_pago",
        "codigo_cliente",
        "empresa",
        "name",
        "r_i_f",
        "cedula",
        "num_admin",
        "admin_email",
        "created_at",
        "clasif",
        "tip_persona",
        "es_contrib",
        "es_juridico",
        "tabulador_islr",
    ],
    disabled=[
        "m_pago",
        "empresa",
        "name",
        "cli_des",
        "r_i_f",
        "cedula",
        "num_admin",
        "admin_email",
        "created_at",
    ],
    use_container_width=True,
)

if st.button(
    "Agregar clientes seleccionados en Profit",
    type="primary",
):
    # Procesar los seleccionados usando la versión editada
    result = add_clientes_en_profit(editor)
    filas_a_mantener = editor[~editor["sel"]]
    st.success(
        f"Clientes agregados exitosamente en Profit. Procesados: {result["derecha_count"]}"
    )
    time.sleep(0.2)
    # Si no quieres resetearlo, simplemente asigna filas_a_mantener
    st.session_state.clientes_para_profit = filas_a_mantener.reset_index(drop=True)
    st.rerun()
