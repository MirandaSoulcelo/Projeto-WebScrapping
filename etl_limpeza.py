"""
ETL e Pré-processamento — Books to Scrape
Execute APÓS o scraper.py ter gerado o arquivo books_raw.csv
"""

import pandas as pd
import numpy as np

print("=" * 50)
print("ETAPA 1 — Carregando dados brutos")
print("=" * 50)

df = pd.read_csv("books_raw.csv", encoding="utf-8")
print(f"Registros carregados: {len(df)}")
print(f"Colunas: {list(df.columns)}")
print(f"\nPrimeiras linhas:")
print(df.head(3).to_string())

registros_antes = len(df)

print("\n" + "=" * 50)
print("ETAPA 2 — Diagnóstico de qualidade")
print("=" * 50)

print("\nValores nulos por coluna:")
print(df.isnull().sum())

print(f"\nDuplicatas encontradas: {df.duplicated().sum()}")
print(f"UPCs únicos: {df['upc'].nunique()}")

print("\nEstatísticas descritivas (campos numéricos):")
print(df[["rating", "price_gbp", "price_incl_tax", "stock_quantity"]].describe().round(2))

print("\n" + "=" * 50)
print("ETAPA 3 — Limpeza e transformações")
print("=" * 50)

antes_dedup = len(df)
df = df.drop_duplicates(subset=["upc"])
print(f"Duplicatas removidas: {antes_dedup - len(df)} linhas")

nulos_antes = df.isnull().sum().sum()

df["stock_quantity"] = df["stock_quantity"].fillna(0).astype(int)

df["category"] = df["category"].fillna("Unknown").str.strip()

df["title"] = df["title"].str.strip()

nulos_depois = df.isnull().sum().sum()
print(f"Valores nulos tratados: {nulos_antes} → {nulos_depois}")

df["rating"] = df["rating"].astype(int)
df["price_gbp"] = df["price_gbp"].astype(float).round(2)
df["price_incl_tax"] = df["price_incl_tax"].astype(float).round(2)
df["in_stock"] = df["in_stock"].astype(str).str.strip()

Q1 = df["price_gbp"].quantile(0.25)
Q3 = df["price_gbp"].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR
outliers_preco = df[(df["price_gbp"] < lower) | (df["price_gbp"] > upper)]
print(f"Outliers de preço detectados (IQR): {len(outliers_preco)} registros")
print(f"  Faixa normal: £{lower:.2f} – £{upper:.2f}")
print(f"  (Mantidos no dataset — outliers reais, não erros)")

print("\n" + "=" * 50)
print("ETAPA 4 — Enriquecimento de dados")
print("=" * 50)

def faixa_preco(p):
    if p < 20:
        return "Barato"
    elif p < 40:
        return "Médio"
    else:
        return "Caro"

df["price_range"] = df["price_gbp"].apply(faixa_preco)
print("Distribuição por faixa de preço:")
print(df["price_range"].value_counts())

rating_labels = {1: "Péssimo", 2: "Ruim", 3: "Regular", 4: "Bom", 5: "Excelente"}
df["rating_label"] = df["rating"].map(rating_labels)

print("\nDistribuição por rating:")
print(df["rating_label"].value_counts())

print("\nDistribuição por categoria (top 10):")
print(df["category"].value_counts().head(10))


print("\n" + "=" * 50)
print("ETAPA 5 — Resumo final da base")
print("=" * 50)

print(f"Registros antes da limpeza : {registros_antes}")
print(f"Registros após limpeza     : {len(df)}")
print(f"Atributos finais           : {len(df.columns)}")

print("\nEstatísticas descritivas finais:")
print(df[["rating", "price_gbp", "price_incl_tax", "stock_quantity"]].describe().round(2))

print("\nValores nulos finais:")
print(df.isnull().sum())

print("\nAmostra final:")
print(df[["title", "category", "rating", "price_gbp", "stock_quantity", "in_stock", "price_range"]].head(5).to_string())


df.to_csv("books_clean.csv", index=False, encoding="utf-8")
print("\n✅ Dataset limpo salvo em: books_clean.csv")
print(f"   {len(df)} registros × {len(df.columns)} colunas")