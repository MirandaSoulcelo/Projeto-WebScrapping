import streamlit as st
import pandas as pd

df = pd.read_csv("books_clean.csv")

st.title(" Dataset de Livros")

categoria = st.selectbox("Filtrar por categoria", ["Todas"] + list(df["category"].unique()))

if categoria != "Todas":
    df = df[df["category"] == categoria]

rating = st.slider("Rating mínimo", 1, 5, 1)
df = df[df["rating"] >= rating]

st.metric("Total de livros", len(df))
st.metric("Preço médio", f"£{df['price_gbp'].mean():.2f}")

st.dataframe(df)

st.bar_chart(df["category"].value_counts())