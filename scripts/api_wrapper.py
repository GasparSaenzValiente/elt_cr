import requests

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
    
    def get_top_players(self, leaderbord_id: int, limit:int=200):
        return self._make_request(f"leaderboard/{leaderbord_id}?limit={limit}")
    
    def get_cards(self):
        """Get all game cards"""
        return self._make_request("cards")