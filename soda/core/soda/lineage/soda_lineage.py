import logging
import os
from soda_lineage.client import OpenLineageClient
from soda.common.config_helper import ConfigHelper

logger = logging.getLogger(__name__)
class SodaLineage:
    """Main entry point for OpenLineage tracing."""
    __instance = None
    soda_config = ConfigHelper.get_instance()

    @staticmethod
    def get_instance(test_mode: bool = False):
        if test_mode:
            SodaLineage.__instance = None

        if SodaLineage.__instance is None:
            SodaLineage(test_mode=test_mode)
        return SodaLineage.__instance

    def __init__(self, config_path=None, test_mode: bool = False):
        if SodaLineage.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            SodaLineage.__instance = self

        # Note that only http authentication is supported at this time
        if config_path:
            pass
        elif os.environ.get('OPENLINEAGE_CONFIG', None):
            pass
        elif os.path.exists(os.path.join(os.getcwd(), 'openlineage.yml')):
            pass
        elif os.path.exists(os.path.join("$HOME", ".openlineage")):
            pass
        # else:
        #     raise Exception("OpenLineage configuration not found. Please set OPENLINEAGE_CONFIG environment variable or create openlineage.yml in current directory or $HOME/.openlineage directory.")

        self.client = OpenLineageClient(url="http://localhost:5000",)
