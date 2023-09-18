# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
        elif item_type == 'event' and item.get('id') is not None and item.get('id').get('tx_digest') \
                and item.get('id').get('event_seq') is not None:
            return concat(item_type, item.get('id').get('tx_digest'), item.get('id').get('event_seq'))
        elif item_type == 'effect' and item.get('transaction_digest') is not None:
            return concat(item_type, item.get('transaction_digest'))

        logging.warning('item_id for item {} is None'.format(json.dumps(item)))

        return None


def concat(*elements):
    return '_'.join([str(elem) for elem in elements])
