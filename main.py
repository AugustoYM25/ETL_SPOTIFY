import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import pandas as pd
from sqlalchemy import create_engine
import time
import random


CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')
DB_HOST = 'db'

auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}')

def buscar_ids_paginado(query_base, limite_total=300):
    
    ids = set()
    offset = 0
    while offset < limite_total:
        try:
            results = sp.search(q=query_base, type='track', limit=50, offset=offset)
            items = results['tracks']['items']
            if not items: break
            
            for item in items:
                for artist in item['artists']:
                    ids.add(artist['id'])
            
            offset += 50
            
            time.sleep(0.1)
        except:
            break
    return list(ids)

def obter_detalhes(ids_chunk, rotulo):
    dados = []
    try:
        artistas = sp.artists(ids_chunk)
        for art in artistas['artists']:
            dados.append({
                'id_spotify': art['id'],
                'nome': art['name'],
                'popularidade': art['popularity'],
                'seguidores': art['followers']['total'],
                'generos': ",".join(art['genres'][:3]),
                'decada_referencia': rotulo
            })
    except:
        pass
    return dados


print("--- INICIANDO EXTRAÇÃO ---")


decadas = [
    (1980, 1989, 'Anos 80'),
    (1990, 1999, 'Anos 90'),
    (2000, 2009, 'Anos 00'),
    (2010, 2019, 'Anos 10'),
    (2020, 2024, 'Atual')
]

generos = ['mpb', 'samba', 'pagode', 'sertanejo', 'rock-brasileiro', 'funk-carioca', 'pop-nacional', 'forro']

todos_dados = []
ids_coletados_total = set() 

for inicio, fim, rotulo in decadas:
    print(f"\n📅 Processando {rotulo} ({inicio}-{fim})...")
    
    for genero in generos:
        
        query = f"genre:{genero} year:{inicio}-{fim}"
        print(f"Buscando: {genero}...", end='\r')
        
        
        novos_ids = buscar_ids_paginado(query, limite_total=100)
        
        
        ids_para_processar = [id for id in novos_ids if id not in ids_coletados_total]
        
        
        chunks = [ids_para_processar[i:i + 50] for i in range(0, len(ids_para_processar), 50)]
        for chunk in chunks:
            detalhes = obter_detalhes(chunk, rotulo)
            todos_dados.extend(detalhes)
            
            for d in detalhes:
                ids_coletados_total.add(d['id_spotify'])

        time.sleep(0.5) 

print(f"\n\n Total bruto coletado: {len(todos_dados)}")

if todos_dados:
    df = pd.DataFrame(todos_dados)
    
    df = df.drop_duplicates(subset=['id_spotify'])
    
    print(f"Salvando {len(df)} artistas únicos no Banco de Dados...")
    df.to_sql('artistas_detalhados', engine, if_exists='replace', index=False)
    print("SUCESSO!.")
else:
    print("Nenhum dado encontrado.")