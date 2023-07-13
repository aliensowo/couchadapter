from django.core.management.base import BaseCommand
from django.utils.translation import gettext as _

from couchadapter.indexer import QueryServiceManager


class Command(BaseCommand):
    help = _("Create Indexes for Query Service")

    def add_arguments(self, parser):
        parser.add_argument(
            "-m",
            "--model",
            help="Run indexing for models",
        )
        parser.add_argument(
            "-p",
            "--primary",
            action="store_true",
            default=False,
            help="Run indexing for models",
        )
        try:
            parser.add_argument(nargs="+", type=str, dest="args")
        except Exception:
            pass

    def handle(self, *args, **options):
        index_manager = QueryServiceManager(model=options.get("model", "help"))
        if options["primary"]:
            index_manager.create_primary()
        else:
            index_manager.create_secondary(*args)
