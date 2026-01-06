from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from django.utils.timezone import now
from typing import Any, Callable, Dict, Hashable, Iterable, List, Optional, Set, TypeVar
from django.db import transaction
from apps.tracker.models import (
    Account,
    Ability,
    Match,
    PlayerAbility,
    PlayerItem,
    PlayerPerformance,
    ShopItem,
)
from .client import DeadlockApiClient
import time as time_mod

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
# Generic helpers
# ------------------------------------------------------------
T = TypeVar("T")

def _dedupe_by_key(rows: List[T], key_fn: Callable[[T], Hashable]) -> List[T]:
    """
    Remove duplicates while preserving order (first occurrence wins).
    """
    seen: Set[Hashable] = set()
    out: List[T] = []
    for r in rows:
        k = key_fn(r)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out

def _ensure_accounts_exist(account_ids: Set[int]) -> None:
    """
    Ensure Account rows exist for all ids. These are NOT notable by default.
    Does not overwrite existing rows/usernames.
    """
    if not account_ids:
        return

    existing = set(Account.objects.filter(account_id__in=account_ids).values_list("account_id", flat=True))
    missing = account_ids - existing
    if not missing:
        return

    Account.objects.bulk_create(
        [Account(account_id=a, username=f"account-{a}", is_notable=False) for a in missing],
        batch_size=2000,
        ignore_conflicts=True,
    )

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
            acc, was_created = Account.objects.get_or_create(
                account_id=account_id,
                defaults={"username": f"account-{account_id}", "is_notable": True},
            )
        if was_created:
            created += 1
        else:
            updated += 1
            # Promote to notable if we learned they're a pro
            if not acc.is_notable:
                acc.is_notable = True
                acc.save(update_fields=["is_notable"])

    return IngestResult(created=created, updated=updated)


def ingest_player_match_history(
    client: DeadlockApiClient,
    account_ids: Iterable[int],
    only_stored_history: bool = True,
    max_matches_per_player: Optional[int] = None,
    since_days: Optional[int] = None,
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

    cutoff_dt = None
    if since_days is not None:
        cutoff_dt = now() - timedelta(days=since_days)
        
    for account_id in account_ids:
        history_payload = client.player_match_history(account_id, only_stored_history=only_stored_history)
        entries = _extract_history_entries(history_payload)

        if since_days is not None:
            entries = [
                e for e in entries
                if _epoch_to_dt_utc(e["start_time"]) >= cutoff_dt
            ]
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


# ------------------------------------------------------------
# Match metadata -> PlayerItem / PlayerAbility
# ------------------------------------------------------------
@dataclass(frozen=True)
class MatchEventsResult:
    match_id: int
    player_items_created: int
    player_abilities_created: int
    unknown_item_ids: int
    deduped_items: int
    deduped_abilities: int


def _extract_player_item_events(metadata_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Expected (based on your description):
    payload["match_info"]["players"][*]["items"][*] contains:
      - item_id
      - game_time_s
      - sold_time_s (optional)
      - upgrade_id (optional)
      - imbued_ability_id (optional)
    """
    match_info = metadata_payload.get("match_info") or {}
    players = match_info.get("players") or []
    events: List[Dict[str, Any]] = []
    for p in players:
        account_id = p.get("account_id")
        for e in (p.get("items") or []):
            events.append({"account_id": account_id, **e})
    return events


def ingest_match_events(
    client: DeadlockApiClient,
    match_ids: Iterable[int],
    replace: bool = False,
) -> Dict[int, MatchEventsResult]:
    """
    Pipeline:
      For each match_id in Match table:
        /v1/matches/<match_id>/metadata
        classify events by item_id membership in Ability vs ShopItem
        insert PlayerAbility + PlayerItem

    Notes:
      - The API may contain duplicate events for the same (account, match, item/ability, time).
        We dedupe in-memory before writing.
      - We also use ignore_conflicts=True as a safety net (idempotent re-runs).
    """
    results: Dict[int, MatchEventsResult] = {}

    # logs
    match_ids = list(match_ids)
    total_matches = len(match_ids)
    start_ts = time_mod.time()
    processed = 0
    failed = 0
    print(f"[INFO] Starting match event ingestion for {total_matches} matches")
    
    # Pull static IDs once for fast classification
    ability_ids = set(Ability.objects.values_list("ability_id", flat=True))
    shop_ids = set(ShopItem.objects.values_list("item_id", flat=True))

    for match_id in match_ids:
        try:
            payload = client.match_metadata(match_id)
        except Exception as e:
            # Gracefully skip corrupted api responses
            failed += 1
            print(
                f"\n[WARN] Skipping match_id={match_id} "
                f"(failed={failed}) error={e}"
            )
            continue

        events = _extract_player_item_events(payload)
        account_ids_in_match = {e.get("account_id") for e in events if e.get("account_id") is not None}
        _ensure_accounts_exist(account_ids_in_match)


        if replace:
            PlayerItem.objects.filter(match_id_id=match_id).delete()
            PlayerAbility.objects.filter(match_id_id=match_id).delete()

        items_to_create: List[PlayerItem] = []
        abilities_to_create: List[PlayerAbility] = []
        unknown_ids: Set[int] = set()

        for e in events:
            account_id = e.get("account_id")
            item_id = e.get("item_id")
            game_time_s = int(e.get("game_time_s") or 0)

            if account_id is None or item_id is None:
                continue

            # Ability upgrade event (item_id maps into Ability table)
            if item_id in ability_ids:
                abilities_to_create.append(
                    PlayerAbility(
                        account_id_id=account_id,
                        match_id_id=match_id,
                        ability_id_id=item_id,
                        game_time=game_time_s,
                    )
                )
                continue

            # ShopItem purchase event (item_id maps into ShopItem table)
            if item_id in shop_ids:
                sold_time_s = int(e.get("sold_time_s") or 0)
                upgrade_id = int(e.get("upgrade_id") or 0)
                imbued_ability_id = int(e.get("imbued_ability_id") or 0)

                # If "0" means none, store NULL
                imbued_fk = imbued_ability_id if imbued_ability_id in ability_ids else None

                items_to_create.append(
                    PlayerItem(
                        account_id_id=account_id,
                        match_id_id=match_id,
                        item_id_id=item_id,
                        game_time=game_time_s,
                        sold_time=sold_time_s,
                        is_upgrade=(upgrade_id != 0),
                        imbued_ability_id=imbued_fk,
                    )
                )
                continue

            unknown_ids.add(item_id)

        # -------------------------
        # Deduplicate within payload
        # -------------------------
        items_before = len(items_to_create)
        abilities_before = len(abilities_to_create)

        items_to_create = _dedupe_by_key(
            items_to_create,
            lambda it: (it.account_id_id, it.match_id_id, it.item_id_id, it.game_time),
        )
        abilities_to_create = _dedupe_by_key(
            abilities_to_create,
            lambda ab: (ab.account_id_id, ab.match_id_id, ab.ability_id_id, ab.game_time),
        )

        items_after = len(items_to_create)
        abilities_after = len(abilities_to_create)

        deduped_items = items_before - items_after
        deduped_abilities = abilities_before - abilities_after

        with transaction.atomic():
            if items_to_create:
                PlayerItem.objects.bulk_create(items_to_create, batch_size=2000, ignore_conflicts=True)
            if abilities_to_create:
                PlayerAbility.objects.bulk_create(abilities_to_create, batch_size=2000, ignore_conflicts=True)

        results[match_id] = MatchEventsResult(
            match_id=match_id,
            player_items_created=items_after,
            player_abilities_created=abilities_after,
            unknown_item_ids=len(unknown_ids),
            deduped_items=deduped_items,
            deduped_abilities=deduped_abilities,
        )
        
        processed += 1
        elapsed = time_mod.time() - start_ts
        avg_per_match = elapsed / processed
        remaining = total_matches - processed
        eta_seconds = int(avg_per_match * remaining)
        print(
            f"\r[PROGRESS] {processed}/{total_matches} matches processed. "
            f"Time Remaining: {eta_seconds}s)",
            end="",
            flush=True,
        )

    return results
