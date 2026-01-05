from django.core.management.base import BaseCommand
from apps.tracker.ingestion.client import DeadlockApiClient
from apps.tracker.ingestion.jobs import ingest_match_events
from apps.tracker.models import Match


class Command(BaseCommand):
    help = "Ingest PlayerItem + PlayerAbility from match metadata for matches in tracker_match."

    def add_arguments(self, parser):
        parser.add_argument(
            "--match-id",
            action="append",
            type=int,
            default=[],
            help="Ingest for a specific match_id. Repeatable: --match-id 492 --match-id 493",
        )
        parser.add_argument(
            "--all-matches",
            action="store_true",
            help="Ingest for all matches in the database.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit how many matches to process (when using --all-matches).",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Delete existing PlayerItem/PlayerAbility rows for each match before inserting.",
        )

    def handle(self, *args, **options):
        client = DeadlockApiClient()

        if options["all_matches"]:
            match_ids = list(Match.objects.values_list("match_id", flat=True))
            if options["limit"] is not None:
                match_ids = match_ids[: options["limit"]]
        else:
            match_ids = options["match_id"]

        if not match_ids:
            self.stdout.write(self.style.ERROR("No match_ids provided. Use --match-id or --all-matches."))
            return

        results = ingest_match_events(client, match_ids=match_ids, replace=options["replace"])

        total_items = sum(r.player_items_created for r in results.values())
        total_abilities = sum(r.player_abilities_created for r in results.values())
        total_unknown = sum(r.unknown_item_ids for r in results.values())

        self.stdout.write(self.style.SUCCESS(
            f"Done. matches={len(results)} PlayerItem={total_items} PlayerAbility={total_abilities} unknown_item_ids={total_unknown}"
        ))
