from django.core.management.base import BaseCommand
from django.db import transaction
from apps.tracker.models import (
    PlayerItem,
    PlayerAbility,
    PlayerPerformance,
    Match,
    Account,
)

class Command(BaseCommand):
    help = "Delete all rows from non-static tables (for local testing)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--yes",
            action="store_true",
            help="Actually perform deletion. Without --yes, prints what would be deleted.",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        do_it = options["yes"]

        counts = {
            "tracker_playeritem": PlayerItem.objects.count(),
            "tracker_playerability": PlayerAbility.objects.count(),
            "tracker_playerperformance": PlayerPerformance.objects.count(),
            "tracker_match": Match.objects.count(),
            "tracker_account": Account.objects.count(),
        }

        self.stdout.write("Counts before reset:")
        for k, v in counts.items():
            self.stdout.write(f"  {k}: {v}")

        if not do_it:
            self.stdout.write(self.style.WARNING("Dry run only. Re-run with --yes to delete."))
            return

        # Delete children first, then parents
        PlayerItem.objects.all().delete()
        PlayerAbility.objects.all().delete()
        PlayerPerformance.objects.all().delete()
        Match.objects.all().delete()
        Account.objects.all().delete()

        self.stdout.write(self.style.SUCCESS("Dynamic tables cleared."))
