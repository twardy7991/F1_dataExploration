import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
import altair as alt

import logging

# class Race_df:
#     def __init__(self, race):
#         self.df = load_data(race)

def load_race_list():
    folder_path = "../data/processed/2023"
    races_list = os.listdir(folder_path)
    
    return races_list
    
def load_data(race):
    df = pd.read_parquet(
        path=f"../data/processed/2023/{race}/session_dataset.parquet",
        engine="pyarrow",
        dtype_backend="pyarrow"
    )
    df["LapTime_s"] = df["LapTime"].dt.total_seconds()
    
    return df

def get_drivers_list(df):
    return df["Driver"].unique()

def plot_laps_altair(df):
    # Chart 1: Lap Times
    lap_time = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("LapNumber:Q"),
            y=alt.Y("LapTime_s:Q", title="Lap Time (s)"), 
            tooltip=["LapNumber", "LapTime_s"]
        )
        .properties(height=200)
    )

    # Chart 2: Acceleration
    acc = (
        alt.Chart(df)
        .transform_fold(
            ["SumLatAcc", "SumLonAcc"],
            as_=["Type", "Value"]
        )
        .mark_line()
        .encode(
            x="LapNumber:Q",
            y=alt.Y("Value:Q", title="Acceleration"), 
            color=alt.Color("Type:N", title="Acceleration type"), 
            tooltip=["LapNumber", "Type:N", "Value:Q"]
        )
        .properties(height=200)
    )

    return lap_time & acc

def get_data_for_driver(df, driver):
    df_2 = df[df["Driver"] == driver]
    
    final_cols = ['LapNumber', 'Driver', 'Compound', 'TyreLife', 'FCL', 'LapTime_s', 'SumLonAcc', 'SumLatAcc']
    
    df_2 = df_2[final_cols].reset_index(drop=True)
    
    return df_2

def main():
    st.set_page_config(
        page_title="F1 Dashboard",
        layout="wide"
    )

    st.title("📊 F1 Dashboard")

    st.write(load_race_list())

    df = load_data("Bahrain Grand Prix")
    
    driver = "VER"
    
    df_2 = get_data_for_driver(df, driver)

    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Data")
        st.dataframe(df_2, height=450)

    with col2:
        st.subheader("Chart")
        st.altair_chart(plot_laps_altair(df_2), use_container_width=True)
        
if __name__ == "__main__":
    main()