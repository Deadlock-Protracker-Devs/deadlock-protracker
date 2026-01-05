from django.core.management.base import BaseCommand
from apps.tracker.ingestion.client import DeadlockApiClient
from apps.tracker.ingestion.jobs import ingest_player_match_history
from apps.tracker.models import Account


class Command(BaseCommand):
    help = "Ingest match-history for Accounts -> upsert Match + PlayerPerformance."

    def add_arguments(self, parser):
        parser.add_argument(
            "--account-id",
            action="append",
            type=int,
            default=[],
            help="Ingest for a specific account_id. Repeatable: --account-id 123 --account-id 456",
        )
        parser.add_argument(
            "--all-accounts",
            action="store_true",
            help="Ingest for all Accounts in the database.",
        )
        parser.add_argument(
            "--max-matches",
            type=int,
            default=None,
            help="Limit matches per player.",
        )
        parser.add_argument(
            "--include-unstored",
            action="store_true",
            help="If set, only_stored_history=false (pulls non-stored history too, if API supports it).",
        )
        parser.add_argument(
            "--since-days",
            type=int,
            default=None,
            help="Only ingest matches from the last N days.",
        )
        parser.add_argument(
            "--all-pros",
            action="store_true",
            help="Ingest for all notable/pro accounts in the database.",
        )

    def handle(self, *args, **options):
        client = DeadlockApiClient()

        only_stored = not options["include_unstored"]

        if options["all_accounts"]:
            account_ids = list(Account.objects.values_list("account_id", flat=True))
        elif options["all_pros"]:
            account_ids = list(Account.objects.filter(is_notable=True).values_list("account_id", flat=True))
        else:
            account_ids = options["account_id"]

        if not account_ids:
            self.stdout.write(self.style.ERROR("No account_ids provided. Use --account-id or --all-accounts."))
            return

        results = ingest_player_match_history(
            client,
            account_ids=account_ids,
            only_stored_history=only_stored,
            max_matches_per_player=options["max_matches"],
            since_days=options["since_days"],
        )

        total_created = sum(r.created for r in results.values())
        total_updated = sum(r.updated for r in results.values())

        self.stdout.write(self.style.SUCCESS(
            f"Done. players={len(results)} matches(created={total_created}, updated={total_updated})"
        ))
