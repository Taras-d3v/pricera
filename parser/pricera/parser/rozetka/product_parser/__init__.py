class RozetkaProductParser:
    @classmethod
    def parse(cls, product_data):
        # Implement parsing logic for Rozetka product data
        parsed_data = {
            "name": product_data.get("title"),
            "price": product_data.get("price"),
            "description": product_data.get("description"),
            "availability": product_data.get("in_stock"),
        }
        return parsed_data
