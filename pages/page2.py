import time
from datetime import datetime
from functools import reduce

import requests
import streamlit as st
from data.mod.ventas.clientes import Clientes
from numpy import where
from pandas import Index, concat

from clients.clientes_crm_ventas import ClientesCRM
from controller.insert_client_mikrowisp import InsertClientes as InsertClientesMW
from controller.insert_client_osticket import InsertClientes as InsertClientesOSTicket
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
    ("count", 0),
]:
    if key not in st.session_state:
        st.session_state[key] = default

if st.button("Refrescar"):
    st.session_state.stage2 = 0


def update_counter(value_change):
    st.session_state.count += value_change


def _prepare_payloads(seleted):
    """Construye las listas de payloads y arrays de actualización a partir de los rows seleccionados."""
    safe_derecha = []
    safe_izquierda = []
    safe_MW = []
    safe_MW_notif = []
    safe_osticket1 = []
    safe_osticket2 = []
    safe_osticket3 = []
    safe_osticket4 = []
    safe_notif_client = []
    rows_update_account = []
    rows_update_referido = []

    oClientesDerecha = Clientes(db=st.session_state.conexion_facturas)
    oClientesIzquierda = Clientes(db=st.session_state.conexion_recibos)
    oInsertClientesMW = InsertClientesMW(db=st.session_state.conexion_mw)
    oInsertClientesOSTicket = InsertClientesOSTicket(
        db=st.session_state.conexion_osticket
    )

    st.session_state.o_clientes_monitoreo_izquierda.new_cod_cliente()
    st.session_state.o_clientes_osticket.new_cod_cliente()
    st.session_state.o_clientes_osticket.new_cod_form_entry()

    for _, row in seleted.iterrows():
        next_cod = st.session_state.o_clientes_monitoreo_izquierda.next_cod_cliente()
        cod_cliente = next_cod if row["codigo_cliente"] == "" else row["codigo_cliente"]
        new_id_cliente_osticket = (
            st.session_state.o_clientes_osticket.next_cod_cliente()
        )
        new_id_form_entry = st.session_state.o_clientes_osticket.next_cod_form_entry()

        if row["clasif"].split("|")[0].strip() == "2":
            cod_cliente = f"{cod_cliente}-0"

        empresa = (
            row["name"]
            if (row["m_pago"] in ("Recibo", "Factura") and row["empresa"] == "")
            or row.get("apartado") == "Referido"
            else row["empresa"]
        )

        user = st.session_state.usuario
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
            "tipo_per": row["tip_persona"].split("|")[0].strip(),
            "co_tab": row["tabulador_islr"].split("|")[0].strip(),
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
            "tipo_adi": row["clasif"].split("|")[0].strip(),
            "valido": 0,
            "sincredito": 0,
            "contribu_e": row["es_contrib"],
            "rete_regis_doc": 0,
            "porc_esp": 100,
            "Id": 0,
            "co_us_in": user,
            "co_sucu_in": "01",
            "co_us_mo": user,
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

        coords = str(row.get("coordenadas_g_p_s", "")).strip()

        texto_mensaje = (
            f"✳️ Cliente: *{cod_cliente}*\n"
            f"razón social: *{empresa}*\n"
            f"módulo: *{row['m_pago']}*\n"
            f"número técnico: *{row['num_tecnico']}*\n"
            f"servicio: *{row['servicio']}*\n"
            f"correo: *{row['tenico_email']}*\n"
            f"coordenadas: {coords}\n"
            f"creado por: *{user}*"
        )
        safe_notif_client.append(
            {
                "mensaje": texto_mensaje,
            }
        )
        safe_MW.append(oInsertClientesMW.normalize_payload_cliente(payload_cliente_MW))

        if row["m_pago"] == "Factura":
            safe_derecha.append(
                oClientesDerecha.normalize_payload_cliente(payload_cliente_profit)
            )
        else:
            safe_izquierda.append(
                oClientesIzquierda.normalize_payload_cliente(payload_cliente_profit)
            )

        if row.get("apartado") == "Cuenta":
            rows_update_account.append({"id": row["id"], "codigo_cliente": cod_cliente})
        elif row.get("apartado") == "Referido":
            rows_update_referido.append(
                {"id": row["id"], "codigo_de_cliente": cod_cliente}
            )

        payload_cliente_osticket = {
            "id": new_id_cliente_osticket,
            "org_id": 0,
            "name": empresa,
            "default_email_id": new_id_cliente_osticket,
            "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        safe_osticket1.append(
            oInsertClientesOSTicket.normalize_payload_cliente(payload_cliente_osticket)
        )

        payload_user_email_ost = {
            "id": new_id_cliente_osticket,
            "user_id": new_id_cliente_osticket,
            "flags": 0,
            "address": row["tenico_email"],
        }
        safe_osticket2.append(
            oInsertClientesOSTicket.normalize_payload_user_email(payload_user_email_ost)
        )

        payload_form_entry = {
            "id": new_id_form_entry,
            "form_id": 1,
            "object_id": new_id_cliente_osticket,
            "object_type": "U",
            "sort": 1,
            "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        safe_osticket3.append(
            oInsertClientesOSTicket.normalize_payload_form_entry(payload_form_entry)
        )

        payload_form_entry_value1 = {
            "entry_id": new_id_form_entry,
            "field_id": 3,
            "value": row["num_tecnico"],
        }
        payload_form_entry_value2 = {
            "entry_id": new_id_form_entry,
            "field_id": 108,
            "value": cod_cliente,
        }
        payload_form_entry_value3 = {
            "entry_id": new_id_form_entry,
            "field_id": 109,
            "value": row["servicio"],
        }
        payload_form_entry_value4 = {
            "entry_id": new_id_form_entry,
            "field_id": 110,
            "value": "ACTIVO",
        }
        payload_form_entry_value5 = {
            "entry_id": new_id_form_entry,
            "field_id": 112,
            "value": row["coordenadas_g_p_s"],
        }

        safe_osticket4.append(
            oInsertClientesOSTicket.normalize_payload_form_entry_values(
                payload_form_entry_value1
            )
        )
        safe_osticket4.append(
            oInsertClientesOSTicket.normalize_payload_form_entry_values(
                payload_form_entry_value2
            )
        )
        safe_osticket4.append(
            oInsertClientesOSTicket.normalize_payload_form_entry_values(
                payload_form_entry_value3
            )
        )
        safe_osticket4.append(
            oInsertClientesOSTicket.normalize_payload_form_entry_values(
                payload_form_entry_value4
            )
        )
        safe_osticket4.append(
            oInsertClientesOSTicket.normalize_payload_form_entry_values(
                payload_form_entry_value5
            )
        )

    return (
        safe_derecha,
        safe_izquierda,
        safe_MW,
        safe_MW_notif,
        safe_notif_client,
        rows_update_account,
        rows_update_referido,
        oInsertClientesMW,
        oInsertClientesOSTicket,
        safe_osticket1,
        safe_osticket2,
        safe_osticket3,
        safe_osticket4,
    )


def add_clientes_en_profit(data):
    derecha_count_rows = 0
    izquierda_count_rows = 0
    # Procesa los clientes seleccionados: separar por módulo y hacer el envío
    # Nota: no usar caché aquí porque la función realiza efectos secundarios
    seleted = data[(data["sel"])].copy()
    oClientesCRM = ClientesCRM(db=st.session_state.conexion_crm)
    oInsertClientesMW = InsertClientesMW(db=st.session_state.conexion_mw)

    if not seleted.empty:
        safe_derecha = []
        safe_izquierda = []
        safe_MW = []
        safe_MW_notif = []
        safe_notif_client = []
        rows_update_account = []
        rows_update_referido = []
        oClientesDerecha = Clientes(db=st.session_state.conexion_facturas)
        oClientesIzquierda = Clientes(db=st.session_state.conexion_recibos)

        # Preparar los payloads de inserción y actualización
        (
            safe_derecha,
            safe_izquierda,
            safe_MW,
            safe_MW_notif,
            safe_notif_client,
            rows_update_account,
            rows_update_referido,
            oInsertClientesMW,
            oInsertClientesOSTicket,
            safe_osticket1,
            safe_osticket2,
            safe_osticket3,
            safe_osticket4,
        ) = _prepare_payloads(seleted)

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

            # Si hay filas para actualizar en apartado cuenta
            if rows_update_account:
                # Actualizar la tabla account en CRM
                oClientesCRM.update_clientes(rows_update_account)
                st.session_state.conexion_crm.commit()

            # Si hay filas para actualizar en apartado referido
            if rows_update_referido:
                # Actualizar la tabla referido en CRM
                oClientesCRM.update_clientes(
                    rows_update_referido, entity_cliente="referido"
                )
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
                    # Este payload se maneja aparte porque requiere el ID generado
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

            # Enviar notificaciones a los clientes
            for notif in safe_notif_client:
                try:
                    st.session_state.oEvolutionClient.send_text(
                        number="584143893828",
                        text=notif["mensaje"],
                        delay=1500,
                    )
                except requests.HTTPError as e:
                    print(
                        f"HTTP error: {e} - response: {getattr(e.response, 'text', None)}"
                    )
                except Exception as e:
                    print(f"Error: {e}")

            rows_count_clientes_osticket = oInsertClientesOSTicket.create_clientes(
                safe_osticket1
            )
            if rows_count_clientes_osticket:
                print(
                    f"filas afectadas en osticket clientes: {rows_count_clientes_osticket}"
                )
                st.session_state.conexion_osticket.commit()
                rows_count_user_email_osticket = (
                    oInsertClientesOSTicket.create_user_email(safe_osticket2)
                )
                if rows_count_user_email_osticket:
                    print(
                        f"filas afectadas en osticket user_email: {rows_count_user_email_osticket}"
                    )
                    st.session_state.conexion_osticket.commit()
                    rows_form_entry_osticket = (
                        oInsertClientesOSTicket.create_os_form_entry(safe_osticket3)
                    )
                    if rows_form_entry_osticket:
                        print(
                            f"filas afectadas en osticket form_entry: {rows_form_entry_osticket}"
                        )
                        st.session_state.conexion_osticket.commit()
                        rows_form_entry_values_osticket = (
                            oInsertClientesOSTicket.create_os_form_entry_values(
                                safe_osticket4
                            )
                        )
                        if rows_form_entry_values_osticket:
                            print(
                                f"filas afectadas en osticket form_entry_values: {rows_form_entry_values_osticket}"
                            )
                            st.session_state.conexion_osticket.commit()
                        else:
                            print(
                                "No se insertaron valores de form_entry_values, se revierte la operación."
                            )
                            st.session_state.conexion_osticket.rollback()
                    else:
                        print(
                            "No se insertaron valores de form_entry, se revierte la operación."
                        )
                        st.session_state.conexion_osticket.rollback()
                else:
                    print(
                        "No se insertaron valores de user_email, se revierte la operación."
                    )
                    st.session_state.conexion_osticket.rollback()
            else:
                print(
                    "No se insertaron valores de form_entry, se revierte la operación."
                )
                st.session_state.conexion_osticket.rollback()

    # Retorna la cantidad de filas procesadas
    return {
        "derecha_count": derecha_count_rows,
        "izquierda_count": izquierda_count_rows,
    }


def set_stage(i):
    st.session_state.stage2 = i


"""
## ⚡👨 Clientes por agregar en PROFIT
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

    # Filtro por tipo de módulo de pago
    if st.session_state.rol_user.has_permission(
        "Add_Clientes_Derecha", "create"
    ) and not st.session_state.rol_user.has_permission(
        "Add_Clientes_Izquierda", "create"
    ):
        # Si el usuario tiene permiso para crear en la derecha pero no en la izquierda
        filtro = clientes["m_pago"] == "Factura"
    elif st.session_state.rol_user.has_permission(
        "Add_Clientes_Izquierda", "create"
    ) and not st.session_state.rol_user.has_permission(
        "Add_Clientes_Derecha", "create"
    ):
        # Si el usuario tiene permiso para crear en la izquierda pero no en la derecha
        filtro = clientes["m_pago"] == "Recibo"
    else:
        # No hay filtro, mostrar todos
        filtro = []

    # Guarda en session state
    st.session_state.clientes_para_profit = clientes[filtro].reset_index(drop=True)
    set_stage(1)


if not st.session_state.clientes_para_profit.empty:
    st.info(
        f" Clientes por agregar: **{len(st.session_state.clientes_para_profit)}**",
        icon="ℹ️",
    )
    # Reemplazar valores NaN por cadenas vacías
    st.session_state.clientes_para_profit.fillna("", inplace=True)

    # Asignar el código de cliente basado en el tipo de módulo de pago y la disponibilidad de RIF o cédula
    df = st.session_state.clientes_para_profit
    # Asegurar que r_i_f no tenga NaN para comparación
    df["r_i_f"] = df["r_i_f"].fillna("")

    condition = (df["m_pago"] == "Factura") & (df["r_i_f"] != "")
    st.session_state.clientes_para_profit["codigo_cliente"] = where(
        condition,
        df["r_i_f"],
        df["cedula"],
    )
    st.session_state.clientes_para_profit["codigo_cliente"] = where(
        df["m_pago"] != "Recibo",
        df["codigo_cliente"],
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
        on_click=update_counter,
        kwargs=dict(value_change=1),
    ):
        # Prevenir múltiples clics
        if st.session_state.count == 1:
            try:
                # Abrir conexiones
                st.session_state.open_cnn_db()

                # Mostrar spinner mientras se procesa
                with st.spinner("Procesando clientes... por favor espera."):
                    # Procesar los seleccionados usando la versión editada
                    result = add_clientes_en_profit(editor)
                    # debug mínimo
                    st.write("Procesamiento finalizado. Resultado:", result)
                filas_a_mantener = editor[~editor["sel"]]
                if result["derecha_count"] > 0 or result["izquierda_count"] > 0:
                    # Si no quieres resetearlo, simplemente asigna filas_a_mantener
                    st.session_state.clientes_para_profit = (
                        filas_a_mantener.reset_index(drop=True)
                    )
                    st.success(
                        "Clientes agregados exitosamente en Profit.",
                        icon="✅",
                    )
                    time.sleep(1)
                    st.rerun()
                else:
                    st.warning("No se pudieron agregar clientes en Profit.", icon="⚠️")
                    time.sleep(1)

                set_stage(1)
            except Exception as e:
                st.error(f"Error al procesar: {e}")
            finally:
                # Asegurar cierre de conexiones y reset del flag en caso de fallo
                try:
                    st.session_state.close_cnn_db()
                    st.session_state.count = 0
                except Exception:
                    pass
                pass
        else:
            st.warning("Presionar el botón solo una vez.", icon="⚠️")
            st.session_state.count = 0

else:
    st.info("No hay clientes por agregar.", icon="ℹ️")
    st.session_state.count = 0


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
