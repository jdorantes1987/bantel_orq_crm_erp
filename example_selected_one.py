# Seleccionar una sola fila mientras se usa st.data_editor con la configuración de columna st.column_config.CheckboxColumn

import streamlit as st
from streamlit import session_state as ss
from pandas import read_csv, DataFrame


if "selected_row_index" not in ss:
    ss.selected_row_index = None


@st.cache_data
def get_data(nrows):
    url = "https://raw.githubusercontent.com/Munees11/Auto-MPG-prediction/master/Scripts/auto_mpg_dataset.csv"
    return read_csv(url, nrows=nrows)


if "df" not in ss:
    ss.df = get_data(10)
    ss.df["selected"] = [False] * len(ss.df)
    ss.df = ss.df[["cylinders", "car_name", "mpg", "selected"]]


def mpg_change():
    edited_rows: dict = ss.mpg["edited_rows"]
    ss.selected_row_index = next(iter(edited_rows))
    ss.df.loc[ss.df["selected"], "selected"] = False
    update_dict = {idx: values for idx, values in edited_rows.items()}
    ss.df.update(DataFrame.from_dict(update_dict, orient="index"))


def main():
    st.markdown("### Data Editor")
    with st.container(border=True):
        st.data_editor(
            ss.df,
            hide_index=True,
            on_change=mpg_change,
            key="mpg",
            width="stretch",
        )
    st.write(f"selected row index: {ss.selected_row_index}")
    if ss.selected_row_index is not None:
        st.write(f"car name: {ss.df.at[ss.selected_row_index, 'car_name']}")


if __name__ == "__main__":
    main()
