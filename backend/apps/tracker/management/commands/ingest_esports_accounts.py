from django.core.management.base import BaseCommand
from apps.tracker.ingestion.client import DeadlockApiClient
from apps.tracker.ingestion.jobs import ingest_esports_accounts


class Command(BaseCommand):
    help = "Ingest esports matches -> match metadata -> account_ids (upsert Account)."

    def add_arguments(self, parser):
        parser.add_argument("--max-matches", type=int, default=None, help="Limit how many esports matches to scan.")

    def handle(self, *args, **options):
        client = DeadlockApiClient()
        res = ingest_esports_accounts(client, max_matches=options["max_matches"])
        self.stdout.write(self.style.SUCCESS(f"Accounts upserted. created={res.created}, updated={res.updated}"))
