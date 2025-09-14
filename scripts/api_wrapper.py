"""
1. API Wrapper para Clash Royale (Python):
    Detalle: Crea un archivo scripts/api_wrapper.py.
        Define una clase ClashRoyaleAPI que tome tu API_KEY en el constructor.
        Métodos:
            _make_request(endpoint): Un método privado que maneje la lógica de la petición HTTP (headers, URL base, manejo de errores comunes).

            get_player_info(player_tag): Recibe un tag y devuelve el JSON del jugador.

            get_clan_info(clan_tag): Recibe un tag y devuelve el JSON del clan.

            get_clan_members(clan_tag): Devuelve la lista de miembros de un clan.

            get_player_battle_log(player_tag): Devuelve el registro de batallas de un jugador.

            get_top_players(limit=200): Opcional, pero muy útil para expandir tu base de datos de jugadores.

            Manejo de errores: Usa try-except para requests.exceptions.RequestException y para códigos de estado HTTP (resp.raise_for_status()). Devuelve None o levanta una excepción personalizada si la petición falla.

    Hito: Archivo api_wrapper.py con las funciones básicas para interactuar con la API.
"""

import requests
import os

class ClashRoyaleAPI:
    BASE_URL = "https://api.clashroyale.com/v1"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API Key cannot be empty.")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }

    def _make_request(self, endpoint: str):
        """ Private method to do http requests"""
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error for {url}: {e.response.status_code} - {e.response.text}")
            return None
        except requests.exceptions.ConnectionError as e:
            print(f"Connection Error for {url}: {e}")
            return None
        except requests.exceptions.Timeout as e:
            print(f"Timeout Error for {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"An unexpected Request Error occurred for {url}: {e}")
            return None

    def get_player_info(self, player_tag: str):
        """Get player info in JSON format"""
        clean_tag = player_tag.lstrip('#').upper()
        return self._make_request(f"players/%23{clean_tag}")
    
    def get_clan_info(self, clan_tag: str):
        """Get player info in JSON format"""
        clean_tag = clan_tag.lstrip('#').upper()
        return self._make_request(f"clans/%23{clean_tag}")        

    def get_clan_members(self, clan_tag: str):
        """Get clan members in JSON format"""
        clean_tag = clan_tag.lstrip('#').upper()
        return self._make_request(f"clans/%23{clean_tag}/members")
    
    def get_player_battle_log(self, player_tag: str):
        """Get player battle log in JSON format"""
        clean_tag = player_tag.lstrip('#').upper()
        return self._make_request(f"players/%23{clean_tag}/battlelog")
    
    def get_top_players(self, leaderbord_id: int ,limit=200):
        return self._make_request(f"leaderboard/{leaderbord_id}")