# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://admin:admin2024@mysql:3306/datamarts")

st.set_page_config(page_title="Home Credit Dashboard", layout="wide")
st.title("Home Credit - Data Platform Bancaire")
st.markdown("**Analyse du risque credit et comportement des clients emprunteurs**")

# KPIs en haut
col1, col2, col3, col4 = st.columns(4)

df_kpi = pd.read_sql("SELECT COUNT(*) as total, SUM(TARGET) as defauts, AVG(AMT_CREDIT) as avg_credit, AVG(AMT_INCOME_TOTAL) as avg_income FROM dm_risque", engine)

with col1:
    st.metric("Total clients", f"{int(df_kpi['total'][0]):,}")
with col2:
    st.metric("Clients en defaut", f"{int(df_kpi['defauts'][0]):,}")
with col3:
    st.metric("Credit moyen", f"{int(df_kpi['avg_credit'][0]):,} €")
with col4:
    st.metric("Revenu moyen", f"{int(df_kpi['avg_income'][0]):,} €")

st.markdown("---")

# ============================================================
# GRAPHIQUE 1 - MARKETING
# ============================================================
st.header("1. Profil des clients sans historique bancaire")
st.markdown("*Problematique : Quels clients sans historique bancaire sont les plus susceptibles de souscrire a un premier credit ?*")

col1, col2 = st.columns(2)

with col1:
    df_marketing = pd.read_sql("""
        SELECT NAME_CONTRACT_TYPE, CODE_GENDER, COUNT(*) as nb_clients,
               AVG(AMT_CREDIT) as credit_moyen,
               AVG(AMT_INCOME_TOTAL) as revenu_moyen
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
        labels={"NAME_CONTRACT_TYPE": "Type de contrat", "nb_clients": "Nombre de clients", "CODE_GENDER": "Genre"},
        color_discrete_map={"F": "#e74c3c", "M": "#3498db"}
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig1b = px.bar(
        df_marketing,
        x="NAME_CONTRACT_TYPE",
        y="credit_moyen",
        color="CODE_GENDER",
        barmode="group",
        title="Credit moyen demande par type de contrat et genre",
        labels={"NAME_CONTRACT_TYPE": "Type de contrat", "credit_moyen": "Credit moyen (€)", "CODE_GENDER": "Genre"},
        color_discrete_map={"F": "#e74c3c", "M": "#3498db"}
    )
    st.plotly_chart(fig1b, use_container_width=True)

st.markdown("---")

# ============================================================
# GRAPHIQUE 2 - RISQUE / ML
# ============================================================
st.header("2. Analyse du risque de defaut de paiement")
st.markdown("*Problematique : Peut-on predire le risque de defaut de paiement d'un client ?*")

col1, col2 = st.columns(2)

with col1:
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

with col2:
    df_risque2 = pd.read_sql("""
        SELECT TARGET,
               AVG(AMT_CREDIT) as credit_moyen,
               AVG(AMT_INCOME_TOTAL) as revenu_moyen,
               AVG(nb_credits_externes) as nb_credits_ext
        FROM dm_risque
        GROUP BY TARGET
    """, engine)
    df_risque2["TARGET"] = df_risque2["TARGET"].map({0: "Pas de defaut", 1: "Defaut"})

    fig2b = px.bar(
        df_risque2,
        x="TARGET",
        y=["credit_moyen", "revenu_moyen"],
        barmode="group",
        title="Credit et revenu moyen par niveau de risque",
        labels={"value": "Montant (€)", "TARGET": "Niveau de risque", "variable": "Indicateur"},
        color_discrete_sequence=["#3498db", "#f39c12"]
    )
    st.plotly_chart(fig2b, use_container_width=True)

st.markdown("---")

# ============================================================
# GRAPHIQUE 3 - BI / DASHBOARD
# ============================================================
st.header("3. Sante du portefeuille de credit par region")
st.markdown("*Problematique : Quelle est la sante globale du portefeuille de credit par region et type de credit ?*")

col1, col2 = st.columns(2)

with col1:
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
        title="Total credit par type de contrat",
        color_continuous_scale="RdYlGn_r",
        labels={"NAME_CONTRACT_TYPE": "Type de contrat", "total_credit": "Total Credit (€)", "taux_defaut": "Taux defaut"}
    )
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    df_bi2 = pd.read_sql("""
        SELECT NAME_CONTRACT_TYPE,
               AVG(taux_defaut) as taux_defaut,
               SUM(nb_defauts) as nb_defauts,
               SUM(nb_clients) as nb_clients
        FROM dm_bi
        GROUP BY NAME_CONTRACT_TYPE
    """, engine)

    fig3b = px.bar(
        df_bi2,
        x="NAME_CONTRACT_TYPE",
        y="taux_defaut",
        color="NAME_CONTRACT_TYPE",
        title="Taux de defaut par type de contrat",
        labels={"NAME_CONTRACT_TYPE": "Type de contrat", "taux_defaut": "Taux de defaut"},
        color_discrete_sequence=["#e74c3c", "#3498db"]
    )
    st.plotly_chart(fig3b, use_container_width=True)

st.markdown("---")
st.success("Dashboard charge avec succes ! Data Platform Home Credit - Mastere Data Engineering EFREI 2026")