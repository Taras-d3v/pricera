from pricera.rozetka.rozetka_product_collector import RozetkaProductCollector
from pricera.hotline.hotline_item_card_collector import HotlineItemCardCollector


DEFAULT_MAPPING = {
    RozetkaProductCollector.payload_key: RozetkaProductCollector,
    HotlineItemCardCollector.payload_key: HotlineItemCardCollector,
}

# explicitly defined payload key to crawler mapping:
PAYLOAD_KEY_TO_CRAWLER = DEFAULT_MAPPING | {}


# explicitly defined payload key to parser mapping:
PAYLOAD_KEY_TO_PARSER = DEFAULT_MAPPING | {}
