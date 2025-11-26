import streamlit as st
import pandas as pd

# 1. Inicializar los datos en session_state si no existen
if "df_datos" not in st.session_state:
    data = {
        "Producto": ["Manzana", "Banana", "Naranja", "Pera", "Uva"],
        "Cantidad": [10, 5, 8, 3, 12],
        "Eliminar": [False, False, False, False, False],  # Columna para el checkbox
    }
    st.session_state.df_datos = pd.DataFrame(data)

st.title("Gestor de Inventario")
st.write("Selecciona la casilla 'Eliminar' y presiona el botón para borrar.")

# 2. Mostrar el data_editor
# Importante: Capturamos el dataframe editado en una variable (df_editado)
df_editado = st.data_editor(
    st.session_state.df_datos,
    column_config={
        "Eliminar": st.column_config.CheckboxColumn(
            "¿Eliminar?",
            help="Selecciona para borrar este ítem",
            default=False,
        )
    },
    disabled=["Producto", "Cantidad"],  # Opcional: Bloquear otras columnas
    hide_index=True,
    key="editor_key",  # Clave única para el widget
)

# 3. Botón para aplicar la eliminación
if st.button("Eliminar Seleccionados", type="primary"):
    # Filtramos: Nos quedamos solo con las filas donde 'Eliminar' es False
    filas_a_mantener = df_editado[df_editado["Eliminar"] == False]

    # 4. Actualizamos el session_state
    # Es vital resetear la columna 'Eliminar' a False en los datos restantes
    # (por si quieres reutilizar la lógica después)
    # Si no quieres resetearlo, simplemente asigna filas_a_mantener
    st.session_state.df_datos = filas_a_mantener.reset_index(drop=True)

    # 5. Forzamos la recarga para que el editor visualice los nuevos datos
    st.rerun()

# Debug: Ver estado actual (opcional)
# st.write("Estado actual del DataFrame:", st.session_state.df_datos)
