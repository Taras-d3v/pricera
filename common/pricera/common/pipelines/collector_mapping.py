from pricera.rozetka import RozetkaProductCrawler, RozetkaProductParser
from pricera.hotline.hotline_item_card_collector import HotlineItemCardCollector
from pricera.rozetka.rozetka_product_parser import RozetkaProductParser

DEFAULT_MAPPING = {
    HotlineItemCardCollector.payload_key: HotlineItemCardCollector,
}

# explicitly defined payload key to crawler mapping:
PAYLOAD_KEY_TO_CRAWLER = DEFAULT_MAPPING | {
    RozetkaProductCrawler.payload_key: RozetkaProductCrawler,
}


# explicitly defined payload key to parser mapping:
PAYLOAD_KEY_TO_PARSER = DEFAULT_MAPPING | {RozetkaProductParser.payload_key: RozetkaProductParser}
