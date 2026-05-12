# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://admin:admin2024@mysql:3306/datamarts")

st.set_page_config(page_title="Home Credit Dashboard", layout="wide")
st.title("Home Credit - Data Platform Dashboard")

# ============================================================
# GRAPHIQUE 1 - MARKETING
# Profil des clients sans historique bancaire
# ============================================================
st.header("1. Profil des clients sans historique bancaire")

df_marketing = pd.read_sql("""
    SELECT NAME_CONTRACT_TYPE, CODE_GENDER, COUNT(*) as nb_clients
    FROM dm_marketing
    GROUP BY NAME_CONTRACT_TYPE, CODE_GENDER
""", engine)

fig1 = px.bar(
    df_marketing,
    x="NAME_CONTRACT_TYPE",
    y="nb_clients",
    color="CODE_GENDER",
    barmode="group",
    title="Clients sans historique par type de contrat et genre",
    labels={"NAME_CONTRACT_TYPE": "Type de contrat", "nb_clients": "Nombre de clients", "CODE_GENDER": "Genre"}
)
st.plotly_chart(fig1, use_container_width=True)

# ============================================================
# GRAPHIQUE 2 - RISQUE / ML
# Repartition des clients par niveau de risque de defaut
# ============================================================
st.header("2. Repartition des clients par niveau de risque de defaut")

df_risque = pd.read_sql("""
    SELECT TARGET, COUNT(*) as nb_clients
    FROM dm_risque
    GROUP BY TARGET
""", engine)
df_risque["TARGET"] = df_risque["TARGET"].map({0: "Pas de defaut", 1: "Defaut"})

fig2 = px.pie(
    df_risque,
    names="TARGET",
    values="nb_clients",
    title="Repartition des clients par risque de defaut",
    color_discrete_sequence=["#2ecc71", "#e74c3c"]
)
st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# GRAPHIQUE 3 - BI / DASHBOARD
# Sante du portefeuille de credit par region et type de credit
# ============================================================
st.header("3. Sante du portefeuille de credit par region")

df_bi = pd.read_sql("""
    SELECT NAME_CONTRACT_TYPE,
           SUM(nb_clients) as nb_clients,
           SUM(total_credit) as total_credit,
           AVG(taux_defaut) as taux_defaut
    FROM dm_bi
    GROUP BY NAME_CONTRACT_TYPE
    ORDER BY total_credit DESC
""", engine)

fig3 = px.bar(
    df_bi,
    x="NAME_CONTRACT_TYPE",
    y="total_credit",
    color="taux_defaut",
    title="Total credit et taux de defaut par type de contrat",
    color_continuous_scale="RdYlGn_r",
    labels={"NAME_CONTRACT_TYPE": "Type de contrat", "total_credit": "Total Credit", "taux_defaut": "Taux defaut"}
)
st.plotly_chart(fig3, use_container_width=True)

st.success("Dashboard charge avec succes !")