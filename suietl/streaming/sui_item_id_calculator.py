import json
import logging


class SuiItemIdCalculator:

    def calculate(self, item):
        if item is None or not isinstance(item, dict):
            return None

        item_type = item.get('type')

        if item_type == 'checkpoint' and item.get('digest') is not None:
            return concat(item_type, item.get('digest'))
        elif item_type == 'transaction' and item.get('digest') is not None:
            return concat(item_type, item.get('digest'))
        elif item_type == 'event' and item.get('transaction_digest') and item.get('event_seq') is not None:
            return concat(item_type, item.get('transaction_digest'), item.get('event_seq'))
        elif item_type == 'object' and item.get('object_id') is not None and item.get('version') is not None:
            return concat(item_type, item.get('object_id'), item.get('version'))

        logging.warning('item_id for item {} is None'.format(json.dumps(item)))

        return None


def concat(*elements):
    return '_'.join([str(elem) for elem in elements])
