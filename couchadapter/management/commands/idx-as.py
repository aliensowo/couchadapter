from django.core.management.base import BaseCommand
from django.utils.translation import gettext as _

from couchadapter.indexer import AnalyticsServiceManager


class Command(BaseCommand):
    help = _("Create Indexes for Analytics Service")

    def add_arguments(self, parser):
        parser.add_argument(
            "-m",
            "--model",
            help="Run indexing for models",
        )
        try:
            parser.add_argument(nargs="+", type=str, dest="args")
        except Exception:
            pass

    def handle(self, *args, **options):
        index_manager = AnalyticsServiceManager(model=options.get("model", "help"))
        index_manager.create_secondary(*args)
