import json
from typing import Any, Dict, Optional

import requests


class EvolutionClient:
    """Cliente simple para enviar solicitudes a la Evolution API.

    Usage:
        client = EvolutionClient(base_url, apikey, instance_name)
        resp = client.send_text(number, text, delay=1200)
    """

    def __init__(
        self,
        base_url: str,
        apikey: str,
        instance_name: str,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.apikey = apikey
        self.instance_name = instance_name
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {"apikey": self.apikey, "Content-Type": "application/json"}
        )

    def _post(self, path: str, payload: Dict[str, Any]) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self.session.post(url, data=json.dumps(payload), timeout=self.timeout)
        resp.raise_for_status()
        return resp

    def send_text(
        self, number: str, text: str, delay: Optional[int] = None, **extra
    ) -> Dict[str, Any]:
        """Enviar un mensaje de texto.

        Args:
            number: número destinatario en formato internacional sin '+' (ej: '58424...')
            text: texto del mensaje
            delay: retardo en milisegundos (opcional)
            extra: campos adicionales que pueden requerir distintas implementaciones
        Returns:
            Decoded JSON response en caso exitoso.
        """
        payload: Dict[str, Any] = {"number": number, "text": text}
        if delay is not None:
            payload["delay"] = delay
        payload.update(extra)

        # Usar el nombre de instancia proporcionado en el constructor
        resp = self._post(f"message/sendText/{self.instance_name}", payload)
        try:
            return resp.json()
        except ValueError:
            return {"status_code": resp.status_code, "text": resp.text}

    def get_information(self) -> Dict[str, Any]:
        """Consultar el estado de un mensaje por su ID.

        Nota: la ruta usada es `message/status/{instance}/{message_id}` —
        ajústala si tu API usa otra ruta.
        """
        url = f"{self.base_url}/"
        resp = self.session.get(url, timeout=self.timeout)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            # devolver info útil en caso de error HTTP
            try:
                return resp.json()
            except ValueError:
                return {"status_code": resp.status_code, "text": resp.text}

        try:
            return resp.json()
        except ValueError:
            return {"status_code": resp.status_code, "text": resp.text}


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    env_path = os.path.join("../conexiones", ".env")

    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el archivo

    # Ejemplo de uso
    url = os.getenv("EVOLUTION_API_URL", "")
    apikey = os.getenv("EVOLUTION_API_KEY", "")
    instance_name = os.getenv("EVOLUTION_INSTANCE_NAME", "")

    # pasar el nombre de la instancia (Alexander en tu caso)
    client = EvolutionClient(
        base_url=url,
        apikey=apikey,
        instance_name=instance_name,
    )
    try:
        response = client.send_text(
            number="584242623485",
            text="Class python: message from API Evolution load_dotenv",
            delay=1200,
        )
        print(response)
        # Ejemplo de consulta de estado (usar message_id real si lo tienes)
        # status = client.get_message_status("<message_id_here>")
        # print(status)
    except requests.HTTPError as e:
        print(f"HTTP error: {e} - response: {getattr(e.response, 'text', None)}")
    except Exception as e:
        print(f"Error: {e}")

    # info = client.get_information()
    # print(info)
