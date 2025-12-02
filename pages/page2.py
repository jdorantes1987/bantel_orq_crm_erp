import time
from datetime import datetime
from functools import reduce

import streamlit as st
from data.mod.ventas.clientes import Clientes
from numpy import where
from pandas import Index, concat

from clients.clientes_crm_ventas import ClientesCRM
from controller.insert_client_mikrowisp import InsertClientes
from helpers.navigation import make_sidebar

# Configuración de página con fondo personalizado
st.set_page_config(
    page_title="Clientes por  agregar",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="",
)
make_sidebar()

# Cargar las claves de session si no existen
for key, default in [
    ("stage2", 0),
    ("clientes_para_profit", None),
    ("editor", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default

if st.button("Refrescar"):
    st.session_state.stage2 = 0


def add_clientes_en_profit(data):
    derecha_count_rows = 0
    izquierda_count_rows = 0
    # Procesa los clientes seleccionados: separar por módulo y hacer el envío
    # Nota: no usar caché aquí porque la función realiza efectos secundarios
    seleted = data[(data["sel"])].copy()
    oClientesCRM = ClientesCRM(db=st.session_state.conexion_crm)
    oInsertClientesMW = InsertClientes(db=st.session_state.conexion_mw)

    if not seleted.empty:
        safe_derecha = []
        safe_izquierda = []
        safe_MW = []
        safe_MW_notif = []
        updates = []
        oClientesDerecha = Clientes(db=st.session_state.conexion_facturas)
        oClientesIzquierda = Clientes(db=st.session_state.conexion_recibos)
        # Inicializar el generador de códigos de cliente de la izquierda
        st.session_state.o_clientes_monitoreo_izquierda.new_cod_cliente()
        for index, row in seleted.iterrows():
            # Generar código de cliente según el módulo, prevee el ingreso manual de código
            cod_cliente = (
                # Si no hay código de cliente para la izquierda, generar uno nuevo
                st.session_state.o_clientes_monitoreo_izquierda.next_cod_cliente()
                if row["codigo_cliente"] == ""
                else (
                    row["codigo_cliente"]
                    if row["m_pago"] == "Recibo"
                    else row["codigo_cliente"]
                )
            )
            if row["m_pago"] in ("Recibo", "Factura") and row["empresa"] == "":
                empresa = row["name"]
            else:
                empresa = row["empresa"]

            payload_cliente_profit = {
                "co_cli": cod_cliente,
                "tip_cli": "01",
                "cli_des": empresa,
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
                "co_pais": "VE",
                "ciudad": "CARACAS",
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

            payload_cliente_MW = {
                # "id": "",
                "nombre": empresa,
                "estado": "ACTIVO",
                "correo": row["tenico_email"],
                "telefono": row["num_tecnico"],
                "movil": row["num_tecnico"],
                "cedula": row["cedula"],
                "pasarela": "s/data",
                "codigo": "123",
                "direccion_principal": row["direccion_tecnica"],
                "codigo_cliente": cod_cliente,
            }

            # Preparar datos para Mikrowisp
            safe_MW.append(
                oInsertClientesMW.normalize_payload_cliente(payload_cliente_MW)
            )

            # Separar por módulo
            if row["m_pago"] == "Factura":
                # Preparar datos para módulo de facturas
                safe_derecha.append(
                    oClientesDerecha.normalize_payload_cliente(payload_cliente_profit)
                )
            else:
                # Preparar datos para módulo de recibos
                safe_izquierda.append(
                    oClientesIzquierda.normalize_payload_cliente(payload_cliente_profit)
                )

            # Prepara los datos para actualizar en CRM
            item = {
                "id": row["id"],
                "codigo_cliente": cod_cliente,
            }
            updates.append(item)

        # Crear clientes en la base de datos
        derecha_count_rows = oClientesDerecha.create_clientes(safe_derecha)
        izquierda_count_rows = oClientesIzquierda.create_clientes(safe_izquierda)

        # Confirmar si ambas inserciones fueron exitosas
        if derecha_count_rows:
            st.session_state.conexion_facturas.commit()
        else:
            st.session_state.conexion_facturas.rollback()

        if izquierda_count_rows:
            st.session_state.conexion_recibos.commit()
        else:
            st.session_state.conexion_recibos.rollback()

        # Actualizar CRM e insertar en Mikrowisp si hubo inserciones
        if derecha_count_rows or izquierda_count_rows:
            # Actualizar CRM
            oClientesCRM.update_clientes(updates)
            st.session_state.conexion_crm.commit()

            # Insertar en Mikrowisp
            rows_count_clientes = oInsertClientesMW.create_clientes(safe_MW)
            if rows_count_clientes:
                st.session_state.conexion_mw.commit()
                # Recorrer los clientes insertados para crear notificaciones
                for cliente in safe_MW:
                    # Obtener el ID de la lista de clientes recién insertados
                    cliente_id = (
                        st.session_state.o_clientes_MW.get_id_cliente_by_codigo(
                            cliente["codigo_cliente"]
                        )
                    )

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

                    # Normalizar y agregar a la lista de notificaciones
                    safe_MW_notif.append(
                        oInsertClientesMW.normalize_payload_notificaciones(
                            payload_notificaciones
                        )
                    )

                # Insertar notificaciones
                rows_count_notificaciones = oInsertClientesMW.create_notificaciones(
                    safe_MW_notif
                )

                # Confirmar o revertir según el resultado
                if rows_count_notificaciones:
                    print(
                        f"filas afectadas en notificaciones: {rows_count_notificaciones}"
                    )
                    st.session_state.conexion_mw.commit()
                else:
                    print("No se insertaron notificaciones, se revierte la operación.")
                    st.session_state.conexion_mw.rollback()
            else:
                st.session_state.conexion_mw.rollback()

    # Cerrar conexiones
    st.session_state.conexion_facturas.close_connection()
    st.session_state.conexion_recibos.close_connection()
    st.session_state.conexion_crm.close_connection()
    st.session_state.conexion_mw.close_connection()

    # Retorna la cantidad de filas procesadas
    return {
        "derecha_count": derecha_count_rows,
        "izquierda_count": izquierda_count_rows,
    }


def set_stage(i):
    st.session_state.stage2 = i


"""
## Clientes por agregar en PROFIT
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
    st.info(
        f" Clientes por agregar: **{len(st.session_state.clientes_para_profit)}**",
        icon="ℹ️",
    )
    # Reemplazar valores NaN por cadenas vacías
    st.session_state.clientes_para_profit.fillna("", inplace=True)

    # Asignar el código de cliente basado en el tipo de módulo de pago
    st.session_state.clientes_para_profit["codigo_cliente"] = where(
        st.session_state.clientes_para_profit["m_pago"] == "Factura",
        st.session_state.clientes_para_profit["r_i_f"],
        "",
    )

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
                    st.session_state.lista_tab
                    if "lista_tab" in st.session_state
                    else []
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
        "Agregar clientes seleccionados",
        type="primary",
    ):
        # Abrir conexiones
        st.session_state.conexion_facturas._connector.connect()
        st.session_state.conexion_recibos._connector.connect()
        st.session_state.conexion_crm._connector.connect()
        st.session_state.conexion_mw._connector.connect()

        # Procesar los seleccionados usando la versión editada
        result = add_clientes_en_profit(editor)
        filas_a_mantener = editor[~editor["sel"]]
        if result["derecha_count"] > 0 or result["izquierda_count"] > 0:
            # Si no quieres resetearlo, simplemente asigna filas_a_mantener
            st.session_state.clientes_para_profit = filas_a_mantener.reset_index(
                drop=True
            )
            st.success(
                f"Clientes agregados exitosamente en Profit. Procesados: {result['derecha_count']}, {result['izquierda_count']}",
                icon="✅",
            )
        else:
            st.warning("No se pudieron agregar clientes en Profit.", icon="⚠️")
            time.sleep(1)
        set_stage(1)
        st.rerun()
else:
    st.info("No hay clientes por agregar.", icon="ℹ️")


if st.session_state.stage2 == 1:
    clients_add_today_izquierda = (
        st.session_state.o_clientes_monitoreo_izquierda.get_clients_inserted_today()
    )

    clients_add_today_derecha = (
        st.session_state.o_clientes_monitoreo_derecha.get_clients_inserted_today()
    )
    # Concatenar ambos DataFrames
    dfs = [
        clients_add_today_izquierda.dropna(axis=1, how="all"),
        clients_add_today_derecha.dropna(axis=1, how="all"),
    ]
    # quedarnos solo con DataFrames no vacíos
    non_empty = [df for df in dfs if not df.empty]

    if non_empty:
        # unificar columnas y reindexar cada df para evitar dtypes sorpresa
        all_cols = reduce(
            lambda a, b: a.union(b), (df.columns for df in non_empty), Index([])
        )
        combined_clients = concat(
            [df.reindex(columns=all_cols) for df in non_empty],
            ignore_index=True,
            sort=False,
        )
        st.text("")
        st.markdown(
            """
        ### ✅ Clientes agregados en Profit el día de hoy
        """
        )
        st.dataframe(
            combined_clients,
            use_container_width=False,
        )
