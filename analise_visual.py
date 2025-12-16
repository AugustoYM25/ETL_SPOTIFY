import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_USER = os.getenv('POSTGRES_USER', 'user')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'password')
DB_NAME = os.getenv('POSTGRES_DB', 'spotify_db')
DB_HOST = 'db'

print("Conectando ao banco para buscar dados...")
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}')
query = "SELECT * FROM artistas_detalhados"
df = pd.read_sql(query, engine)

print(f"Dados carregados: {len(df)} artistas encontrados.")

if not os.path.exists('graficos'):
    os.makedirs('graficos')

sns.set_theme(style="whitegrid")
plt.figure(figsize=(12, 6))

generos_series = df['generos'].str.split(',', expand=True).stack().reset_index(level=1, drop=True)
generos_series = generos_series.str.strip()
generos_series = generos_series[generos_series != ""]

top_generos = generos_series.value_counts().head(15)

plt.figure(figsize=(12, 8))
sns.barplot(x=top_generos.values, y=top_generos.index, palette="viridis")
plt.title('Top 15 Gêneros Musicais Coletados', fontsize=16)
plt.xlabel('Quantidade de Artistas')
plt.ylabel('Gênero')
plt.tight_layout()
plt.savefig('graficos/1_top_generos.png')
print("Grafico 1 salvo: graficos/1_top_generos.png")

plt.figure(figsize=(10, 6))
sns.scatterplot(data=df, x='popularidade', y='seguidores', hue='decada_referencia', alpha=0.6)
plt.yscale('log')
plt.title('Relação Popularidade vs Seguidores (por Década)', fontsize=16)
plt.xlabel('Popularidade (0-100)')
plt.ylabel('Seguidores (Escala Log)')
plt.legend(title='Década')
plt.tight_layout()
plt.savefig('graficos/2_pop_vs_seg.png')
print("Grafico 2 salvo: graficos/2_pop_vs_seg.png")

plt.figure(figsize=(8, 5))
contagem_decada = df['decada_referencia'].value_counts()
plt.pie(contagem_decada, labels=contagem_decada.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette("pastel"))
plt.title('Distribuição de Artistas por Década', fontsize=16)
plt.savefig('graficos/3_pizza_decadas.png')
print("Grafico 3 salvo: graficos/3_pizza_decadas.png")

print("\nAnalise concluida! Verifique a pasta 'graficos'.")