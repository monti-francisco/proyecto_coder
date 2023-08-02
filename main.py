#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from psycopg2 import Error
import sqlalchemy
from sqlalchemy import create_engine, text
import psycopg2

# conexion a redshift
user = "montione457_coderhouse"
password = "xrZ4lz91MT"
host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
port = "5439"
database = "data-engineer-database"
conn = psycopg2.connect(host=host, dbname=database, user=user, password=password, port=port )

#funcion para traer los datos desde la api
def get_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener los datos del Pokémon {pokemon_id}")
        return None

#funcion para crear el dataframe a partir de una lista de ids de pokemons
def create_pokemon_dataframe(pokemon_ids):
    data = []
    for pokemon_id in pokemon_ids:
        pokemon_data = get_pokemon_data(pokemon_id)
        if pokemon_data:
            data.append({
                "Nombre": pokemon_data["name"],
                "Altura": pokemon_data["height"],
                "Peso": pokemon_data["weight"],
                "Tipos": ", ".join([t["type"]["name"] for t in pokemon_data["types"]]),
                "Habilidades": ", ".join([a["ability"]["name"] for a in pokemon_data["abilities"]]),
            })

    df = pd.DataFrame(data)

    # Duplicar la primera fila y agregarla al DataFrame (Esto lo hago para poder despues hacer el proceso de ETL, ya que la API no posee duplicados)
    duplicado = df.iloc[[0]]
    df = pd.concat([df, duplicado], ignore_index=True)
    return df

def run_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        columnas = [description[0] for description in cursor.description]
        result = cursor.fetchall()
        return pd.DataFrame(result, columns=columnas)
    except Error as e:
        print(f"Error '{e}' ha ocurrido")

#proceso de consumo
# Lista de IDs de Pokémon que deseas consultar
pokemon_ids = range(1,15)

# Crear el DataFrame
pokemon_df = create_pokemon_dataframe(pokemon_ids)

# Mostrar el DataFrame
print(pokemon_df)

#limpieza de datos
#verificacion de datos
#print(pokemon_df.duplicated().sum())
#print(pokemon_df.isnull().sum())
print("duplicados antes de la limpieza: ", pokemon_df.duplicated().sum())
pokemon_df = pokemon_df.dropna()
pokemon_df = pokemon_df.drop_duplicates()
print("duplicados despues de la limpieza: ", pokemon_df.duplicated().sum())

#query = "select * from montione457_coderhouse.pokemons"
#print(run_query(conn, query))
#carga de datos a redshift
connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
engine = sqlalchemy.create_engine(connection_string)
pokemon_df.to_sql("pokemons", engine, schema="montione457_coderhouse", if_exists='replace', index=False, index_label=None, method=None)