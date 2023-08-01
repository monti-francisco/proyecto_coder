#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd

def get_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener los datos del Pokémon {pokemon_id}")
        return None

def create_pokemon_dataframe(pokemon_ids):
    data = []
    for pokemon_id in pokemon_ids:
        pokemon_data = get_pokemon_data(pokemon_id)
        if pokemon_data:
            data.append({
                "Nombre": pokemon_data["name"],
                "Altura": pokemon_data["height"],
                "Peso": pokemon_data["weight"],
                "Tipo(s)": ", ".join([t["type"]["name"] for t in pokemon_data["types"]]),
                "Habilidades": ", ".join([a["ability"]["name"] for a in pokemon_data["abilities"]]),
            })

    df = pd.DataFrame(data)
    return df

# Lista de IDs de Pokémon que deseas consultar
pokemon_ids = [1, 4, 7, 25, 133, 150, 151]

# Crear el DataFrame
pokemon_df = create_pokemon_dataframe(pokemon_ids)

# Mostrar el DataFrame
print(pokemon_df)

