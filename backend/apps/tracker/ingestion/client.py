# requests to public APIs
import time
from typing import Any, Dict, List, Optional
import requests
from django.conf import settings

class DeadlockApiClient:
    """
    Thin HTTP wrapper around the Deadlock API.
    Keeps request logic in one place (timeouts, retries, rate limiting).
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout_s: Optional[float] = None,
        sleep_s: Optional[float] = None,
        max_retries: int = 3,
    ) -> None:
        self.base_url = (base_url or settings.DEADLOCK_API_BASE_URL).rstrip("/")
        self.timeout_s = timeout_s if timeout_s is not None else settings.DEADLOCK_API_TIMEOUT_S
        self.sleep_s = sleep_s if sleep_s is not None else settings.DEADLOCK_API_SLEEP_S
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout_s)

                # Basic backoff on 429 / 5xx
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(self.sleep_s * attempt)
                    continue

                resp.raise_for_status()
                time.sleep(self.sleep_s)  # gentle pacing
                return resp.json()

            except Exception as e:
                last_err = e
                time.sleep(self.sleep_s * attempt)

        raise RuntimeError(f"GET {url} failed after {self.max_retries} retries: {last_err}")

    # ---- Endpoints ----

    def esports_matches(self) -> Any:
        # Returns list of match objects or ids (depends on API); we parse defensively later.
        return self._get("/v1/esports/matches")

    def match_metadata(self, match_id: int) -> Any:
        return self._get(f"/v1/matches/{match_id}/metadata")

    def player_match_history(self, account_id: int, only_stored_history: bool = True) -> Any:
        return self._get(
            f"/v1/players/{account_id}/match-history",
            params={"only_stored_history": str(only_stored_history).lower()},
        )
