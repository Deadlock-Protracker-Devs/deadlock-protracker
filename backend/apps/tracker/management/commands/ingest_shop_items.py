from __future__ import annotations
import csv
from dataclasses import dataclass
from random import sample
from typing import Dict, List, Set, Tuple
from django.core.management.base import BaseCommand
from django.db import transaction
from apps.tracker.models import ShopItem, ShopItemUpgrade


@dataclass(frozen=True)
class ImportStats:
    created: int = 0
    updated: int = 0


def _parse_bool(raw: str) -> bool:
    """
    Accept common CSV boolean encodings.
    """
    s = (raw or "").strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean value: {raw!r} (expected true/false, 1/0, yes/no)")


class Command(BaseCommand):
    help = "Ingest ShopItem + ShopItemUpgrade from two CSV files (idempotent)."

    def add_arguments(self, parser):
        parser.add_argument(
            "items_csv",
            type=str,
            help="Path to shop_items.csv (columns: item_id,name,icon_key,imbue,type,cost)",
        )
        parser.add_argument(
            "upgrades_csv",
            type=str,
            help="Path to shop_items_upgrades.csv (columns: from_item,to_item)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse and validate, but do not write to the database.",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        items_csv: str = options["items_csv"]
        upgrades_csv: str = options["upgrades_csv"]
        dry_run: bool = options["dry_run"]

        # -------------------------
        # 1) Ingest / upsert items
        # -------------------------
        with open(items_csv, newline="", encoding="utf-8") as f:
            sample = f.read(4096)
            f.seek(0)
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
            reader = csv.DictReader(f, dialect=dialect)
            required = {"item_id", "name", "icon_key", "imbue", "type", "cost"}
            if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
                raise ValueError(f"shop_items.csv must contain headers: {sorted(required)}. Got: {reader.fieldnames}")

            rows = list(reader)

        # Convert + validate
        item_ids: List[int] = []
        incoming: Dict[int, Dict] = {}
        for i, row in enumerate(rows, start=2):  # header is line 1
            try:
                item_id = int(row["item_id"])
                name = (row["name"] or "").strip()
                icon_key = (row["icon_key"] or "").strip()
                imbue = _parse_bool(row["imbue"])
                item_type = (row["type"] or "").strip()
                cost = int(row["cost"])
            except Exception as e:
                raise ValueError(f"Invalid row in shop_items.csv at line {i}: {row!r}. Error: {e}") from e

            if not name:
                raise ValueError(f"Empty name in shop_items.csv at line {i} (item_id={item_id})")

            # validate type against choices
            valid_types = {c[0] for c in ShopItem.ItemType.choices}
            if item_type not in valid_types:
                raise ValueError(
                    f"Invalid type in shop_items.csv at line {i} (item_id={item_id}): {item_type!r}. "
                    f"Expected one of: {sorted(valid_types)}"
                )

            item_ids.append(item_id)
            incoming[item_id] = {
                "item_id": item_id,
                "name": name,
                "icon_key": icon_key,
                "imbue": imbue,
                "type": item_type,
                "cost": cost,
            }

        existing_by_id: Dict[int, ShopItem] = {
            si.item_id: si for si in ShopItem.objects.filter(item_id__in=item_ids)
        }

        to_create: List[ShopItem] = []
        to_update: List[ShopItem] = []

        for item_id, data in incoming.items():
            existing = existing_by_id.get(item_id)
            if existing is None:
                to_create.append(ShopItem(**data))
            else:
                changed = False
                for field in ["name", "icon_key", "imbue", "type", "cost"]:
                    new_val = data[field]
                    if getattr(existing, field) != new_val:
                        setattr(existing, field, new_val)
                        changed = True
                if changed:
                    to_update.append(existing)

        if dry_run:
            self.stdout.write(self.style.WARNING(
                f"[DRY RUN] ShopItems: would create={len(to_create)} update={len(to_update)}"
            ))
        else:
            if to_create:
                ShopItem.objects.bulk_create(to_create, batch_size=1000)
            if to_update:
                ShopItem.objects.bulk_update(
                    to_update,
                    fields=["name", "icon_key", "imbue", "type", "cost"],
                    batch_size=1000,
                )

            self.stdout.write(self.style.SUCCESS(
                f"ShopItems: created={len(to_create)} updated={len(to_update)} (total_in_csv={len(incoming)})"
            ))

        # Ensure we can resolve items for upgrades (includes newly created ones)
        all_items_by_id: Dict[int, ShopItem] = {
            si.item_id: si for si in ShopItem.objects.filter(item_id__in=item_ids)
        }

        # --------------------------------
        # 2) Ingest upgrade edges (from->to)
        # --------------------------------
        with open(upgrades_csv, newline="", encoding="utf-8") as f:
            sample = f.read(4096)
            f.seek(0)
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
            reader = csv.DictReader(f, dialect=dialect)
            required = {"from_item", "to_item"}
            if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
                raise ValueError(
                    f"shop_items_upgrades.csv must contain headers: {sorted(required)}. Got: {reader.fieldnames}"
                )
            upgrade_rows = list(reader)

        edges_in_csv: Set[Tuple[int, int]] = set()
        for i, row in enumerate(upgrade_rows, start=2):
            try:
                from_id = int(row["from_item"])
                to_id = int(row["to_item"])
            except Exception as e:
                raise ValueError(f"Invalid row in shop_items_upgrades.csv at line {i}: {row!r}. Error: {e}") from e

            if from_id == to_id:
                raise ValueError(f"Self-upgrade edge at line {i}: {from_id} -> {to_id} is not allowed")

            # Both ends must exist (either in CSV items or already in DB).
            if from_id not in all_items_by_id:
                # allow references to existing DB items not in items_csv:
                from_obj = ShopItem.objects.filter(item_id=from_id).first()
                if not from_obj:
                    raise ValueError(f"Unknown from_item at line {i}: {from_id} (not in DB and not in items_csv)")
                all_items_by_id[from_id] = from_obj

            if to_id not in all_items_by_id:
                to_obj = ShopItem.objects.filter(item_id=to_id).first()
                if not to_obj:
                    raise ValueError(f"Unknown to_item at line {i}: {to_id} (not in DB and not in items_csv)")
                all_items_by_id[to_id] = to_obj

            edges_in_csv.add((from_id, to_id))

        # Fetch existing edges for these from/to ids so re-runs don't duplicate
        from_ids = {a for a, _ in edges_in_csv}
        to_ids = {b for _, b in edges_in_csv}

        existing_edges = set(
            ShopItemUpgrade.objects.filter(
                from_item_id__in=from_ids,
                to_item_id__in=to_ids,
            ).values_list("from_item_id", "to_item_id")
        )

        edges_to_create = edges_in_csv - existing_edges

        upgrades_to_create = [
            ShopItemUpgrade(
                from_item_id=from_id,
                to_item_id=to_id,
            )
            for (from_id, to_id) in edges_to_create
        ]

        if dry_run:
            self.stdout.write(self.style.WARNING(
                f"[DRY RUN] ShopItemUpgrades: would create={len(upgrades_to_create)} (csv_edges={len(edges_in_csv)})"
            ))
            # In dry-run, rollback everything by raising to exit atomic block cleanly.
            raise SystemExit(0)

        if upgrades_to_create:
            ShopItemUpgrade.objects.bulk_create(upgrades_to_create, batch_size=2000)

        self.stdout.write(self.style.SUCCESS(
            f"ShopItemUpgrades: created={len(upgrades_to_create)} (csv_edges={len(edges_in_csv)}, existing={len(existing_edges)})"
        ))
