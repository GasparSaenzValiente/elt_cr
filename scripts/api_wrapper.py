import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)


class ClashRoyaleAPI:
    """
    Cliente para la API oficial de Clash Royale v1.

    Encapsula la autenticación y el manejo de errores HTTP,
    exponiendo métodos tipados para cada endpoint utilizado
    en el pipeline ELT.

    Attributes:
        BASE_URL: URL base de la API de Clash Royale.
        api_key:  Clave de autenticación Bearer.
        headers:  Cabeceras HTTP enviadas en cada request.
    """

    BASE_URL = "https://api.clashroyale.com/v1"

    def __init__(self, api_key: str) -> None:
        """
        Inicializa el cliente con la clave de API.

        Args:
            api_key: Token Bearer obtenido en https://developer.clashroyale.com/

        Raises:
            ValueError: Si api_key es una cadena vacía o None.
        """
        if not api_key:
            raise ValueError("API Key cannot be empty.")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    # ------------------------------------------------------------------
    # Métodos privados
    # ------------------------------------------------------------------

    def _encode_tag(self, tag: str) -> str:
        """
        Limpia y URL-encodea un tag de jugador o clan.

        La API requiere que el símbolo '#' se represente como '%23'.

        Args:
            tag: Tag con o sin el prefijo '#', en cualquier capitalización.

        Returns:
            Tag en mayúsculas con '#' reemplazado por '%23'.
        """

        return tag.lstrip("#").upper()

    def _make_request(self, endpoint: str) -> Optional[dict]:
        """
        Realiza un GET al endpoint indicado y retorna el JSON parseado.

        Maneja todos los casos de error de requests de forma granular,
        logueando el error y retornando None para que el caller decida
        si continuar o abortar.

        Args:
            endpoint: Path relativo al BASE_URL (sin barra inicial).

        Returns:
            Diccionario con la respuesta JSON, o None si ocurrió algún error.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(
                "HTTP Error for %s: %s - %s",
                url,
                e.response.status_code,
                e.response.text,
            )
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error("Connection Error for %s: %s", url, e)
            return None
        except requests.exceptions.Timeout:
            logger.error("Timeout Error for %s (limit: 10s)", url)
            return None
        except requests.exceptions.RequestException as e:
            logger.error("Unexpected Request Error for %s: %s", url, e)
            return None

    # ------------------------------------------------------------------
    # Endpoints públicos
    # ------------------------------------------------------------------

    def get_player_info(self, player_tag: str) -> Optional[dict]:
        """
        Obtiene el perfil completo de un jugador.

        Args:
            player_tag: Tag del jugador (ej. '#2PPCJ0UUP' o '2PPCJ0UUP').

        Returns:
            Diccionario con los datos del jugador, o None si falló el request.
        """
        clean_tag = self._encode_tag(player_tag)
        return self._make_request(f"players/%23{clean_tag}")

    def get_clan_info(self, clan_tag: str) -> Optional[dict]:
        """
        Obtiene la información de un clan.

        Args:
            clan_tag: Tag del clan (ej. '#2L80YUL' o '2L80YUL').

        Returns:
            Diccionario con los datos del clan, o None si falló el request.
        """
        clean_tag = self._encode_tag(clan_tag)
        return self._make_request(f"clans/%23{clean_tag}")

    def get_clan_members(self, clan_tag: str) -> Optional[dict]:
        """
        Obtiene la lista de miembros de un clan.

        Args:
            clan_tag: Tag del clan (ej. '#2L80YUL' o '2L80YUL').

        Returns:
            Diccionario con la lista 'items' de miembros, o None si falló.
        """
        clean_tag = self._encode_tag(clan_tag)
        return self._make_request(f"clans/%23{clean_tag}/members")

    def get_player_battle_log(self, player_tag: str) -> Optional[list]:
        """
        Obtiene el historial de batallas recientes de un jugador.

        Args:
            player_tag: Tag del jugador (ej. '#2PPCJ0UUP' o '2PPCJ0UUP').

        Returns:
            Lista de batallas o None si falló el request.
        """
        clean_tag = self._encode_tag(player_tag)
        return self._make_request(f"players/%23{clean_tag}/battlelog")

    def get_cards(self) -> Optional[dict]:
        """
        Obtiene el catálogo completo de cartas del juego.

        Returns:
            Diccionario con las claves 'items' (cartas estándar) y
            'supportItems' (cartas de soporte), o None si falló.
        """
        return self._make_request("cards")

    def get_top_clans_global(
        self, limit: int = 10, location_id: str = "global"
    ) -> Optional[dict]:
        """
        Obtiene el ranking de los mejores clanes para una ubicación dada.

        Args:
            limit:       Cantidad de clanes a retornar (default: 10, max: 200).
            location_id: ID de ubicación o 'global' para el ranking mundial.

        Returns:
            Diccionario con la clave 'items' con la lista de clanes, o None.
        """
        return self._make_request(
            f"locations/{location_id}/rankings/clans?limit={limit}"
        )
