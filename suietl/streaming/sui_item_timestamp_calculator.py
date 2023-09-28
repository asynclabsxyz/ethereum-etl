import json
import logging
from datetime import datetime


class SuiItemTimestampCalculator:
    def calculate(self, item):
        if item is None or not isinstance(item, dict):
            return None

        if item.get('timestamp') is not None:
            return item.get('timestamp')

        logging.warning('item_timestamp for item {} is None'.format(json.dumps(item)))

        return None
