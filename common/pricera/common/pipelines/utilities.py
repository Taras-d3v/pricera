from typing import Iterator, Tuple
import logging
from copy import deepcopy
from pricera.common import ensure_list
from pricera.common.collectors import BaseCollector

logger = logging.getLogger("pipeline_utilities")


def prepare_message(
    collector_mapping: dict[str, BaseCollector], original_message: dict
) -> Iterator[Tuple["BaseCollector", dict]]:
    payload = original_message.get("payload")
    if not payload:
        logger.warning("Message payload is empty. Skipping pipeline processing")
        return

    for payload_key, payload_values in payload.items():
        collector_cls = collector_mapping.get(payload_key)
        if not collector_cls:
            logger.error("No collector found for payload key. Skipping.", extra={"payload_key": payload_key})
            continue

        payload_values = ensure_list(payload_values)
        # if parser/crawler can only handle single messages, yield each payload value separately
        if collector_cls.is_synchronous:
            for payload_value in payload_values:
                single_message = deepcopy(original_message)
                single_message["payload"] = {payload_key: payload_value}
                yield collector_cls, single_message
        # else, yield the entire batch
        else:
            batch_message = deepcopy(original_message)
            batch_message["payload"] = {payload_key: payload_values}
            yield collector_cls, batch_message
