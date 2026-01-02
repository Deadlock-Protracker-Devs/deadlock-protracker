from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set
from django.db import transaction
from apps.tracker.models import Account, Match, PlayerPerformance
from .client import DeadlockApiClient

def _epoch_to_dt_utc(epoch_s: int) -> datetime:
    """Convert epoch seconds -> aware UTC datetime."""
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc)


def _extract_match_ids(esports_payload: List[Dict[str, Any]]) -> List[int]:
    """
    Expected shape:
    [
      {"match_id": 39080962, "status": "Completed", ...},
      ...
    ]
    """
    return [m["match_id"] for m in esports_payload if m["status"] == "Completed"]


def _extract_account_ids(metadata_payload: Dict[str, Any]) -> List[int]:
    """
    Expected shape:
    {
      "match_info": {
        "players": [
          {"account_id": 1124397375, ...},
          ...
        ]
      }
    }
    """
    return [p["account_id"] for p in metadata_payload["match_info"]["players"]]


def _extract_history_entries(history_payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Expected shape:
    [
      {"account_id": ..., "match_id": ..., "start_time": ..., ...},
      ...
    ]
    """
    return history_payload


@dataclass(frozen=True)
class IngestResult:
    created: int
    updated: int


# ------------------------------------------------------------
# Jobs
# ------------------------------------------------------------
def ingest_esports_accounts(client: DeadlockApiClient, max_matches: Optional[int] = None) -> IngestResult:
    """
    Pipeline:
      /v1/esports/matches -> match_ids
      /v1/matches/<match_id>/metadata -> account_ids
      Upsert Account rows (username might be unknown here)
    """
    esports_payload = client.esports_matches()
    match_ids = _extract_match_ids(esports_payload)

    if max_matches is not None:
        match_ids = match_ids[:max_matches]

    account_ids: Set[int] = set()
    for match_id in match_ids:
        metadata = client.match_metadata(match_id)
        account_ids.update(_extract_account_ids(metadata))

    created = 0
    updated = 0

    # If you don't want to overwrite curated usernames later, you can switch
    # this to get_or_create() instead of update_or_create().
    with transaction.atomic():
        for account_id in account_ids:
            _, was_created = Account.objects.update_or_create(
                account_id=account_id,
                defaults={"username": f"account-{account_id}"},
            )
            if was_created:
                created += 1
            else:
                updated += 1

    return IngestResult(created=created, updated=updated)


def ingest_player_match_history(
    client: DeadlockApiClient,
    account_ids: Iterable[int],
    only_stored_history: bool = True,
    max_matches_per_player: Optional[int] = None,
) -> Dict[int, IngestResult]:
    """
    Pipeline:
      /v1/players/<account_id>/match-history?only_stored_history=true
      Upsert Match and PlayerPerformance (from each history entry).

    Notes:
      - We avoid static ingestion (Hero/Item/Rank) for now.
      - Match.avg_rank must be nullable OR you must set a placeholder.
      - Match.date must be writable (not auto_now_add) if you want API start_time.
    """
    results: Dict[int, IngestResult] = {}

    for account_id in account_ids:
        history_payload = client.player_match_history(account_id, only_stored_history=only_stored_history)
        entries = _extract_history_entries(history_payload)

        if max_matches_per_player is not None:
            entries = entries[:max_matches_per_player]

        created = 0
        updated = 0

        # Ensure Account exists (keeps DB consistent for FK inserts)
        Account.objects.update_or_create(
            account_id=account_id,
            defaults={"username": f"account-{account_id}"},
        )

        with transaction.atomic():
            for e in entries:
                # ---- Match fields (from history entry) ----
                match_id: int = e["match_id"]
                start_time: int = e["start_time"]
                duration_s: int = e["match_duration_s"]

                match_defaults = {
                    "date": _epoch_to_dt_utc(start_time),
                    "duration": timedelta(seconds=duration_s),
                }

                _, match_created = Match.objects.update_or_create(
                    match_id=match_id,
                    defaults=match_defaults,
                )
                if match_created:
                    created += 1
                else:
                    updated += 1

                # ---- PlayerPerformance fields (from history entry) ----
                kills: int = e["player_kills"]
                deaths: int = e["player_deaths"]
                assists: int = e["player_assists"]
                networth: int = e["net_worth"]
                team: int = e["player_team"]
                match_result: int = e["match_result"]

                is_win = (team == match_result)

                PlayerPerformance.objects.update_or_create(
                    account_id_id=account_id,
                    match_id_id=match_id,
                    defaults={
                        "kills": kills,
                        "deaths": deaths,
                        "assists": assists,
                        "networth": networth,
                        "team": team,
                        "is_win": is_win,
                    },
                )

        results[account_id] = IngestResult(created=created, updated=updated)

    return results
