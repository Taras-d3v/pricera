from typing import Iterator, Tuple, Optional
import logging
from copy import deepcopy
from pricera.common import ensure_list
from pricera.common.collectors import BaseCollector

logger = logging.getLogger("pipeline_utilities")


def prepare_message(
    collector_mapping: dict[str, BaseCollector], original_message: dict
) -> Iterator[Optional[Tuple["BaseCollector", dict]]]:
    payload = original_message.get("payload")
    if not payload:
        logger.warning("Message payload is empty. Skipping pipeline processing")
        # todo: fixme
        return

    for payload_key, payload_values in payload.items():
        collector_cls = collector_mapping.get(payload_key)
        if not collector_cls:
            logger.error("No collector found for payload key. Skipping.", extra={"payload_key": payload_key})
            continue

        payload_values = ensure_list(payload_values)
        # if parser/crawler can handle batch processing, yield the original message with all payload values
        if not collector_cls.is_synchronous:
            original_message["payload"] = {payload_key: payload_values}
            yield collector_cls, original_message
        # else, yield individual messages for each payload value
        else:
            for payload_value in payload_values:
                single_message = deepcopy(original_message)
                single_message["payload"] = {payload_key: payload_value}
                yield collector_cls, single_message
