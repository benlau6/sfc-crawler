# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class FilterSFCFirmPipeline:
    def process_item(self, item, spider):
        """
        SFC item fields are spread in multiple pages
        this pipeline drops the item while it is in the intermediate stage
        and an item will eventually passthrough
        when it contains all the fields across different pages
        """
        fields = ["tel", "email", "conditions", "disciplinary_actions"]
        item_keys = item.keys()
        for field in fields:
            if field not in item_keys:
                raise DropItem(f"Missing {field}")

        return item


class MongoFirmPipeline:
    # TODO: consider professionals
    collection = "firms"

    def __init__(self, mongo_host, mongo_db, mongo_username, mongo_password):
        self.mongo_host = mongo_host
        self.mongo_db = mongo_db
        self.mongo_username = mongo_username
        self.mongo_password = mongo_password

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_host=crawler.settings.get("MONGO_HOST", "localhost"),
            mongo_db=crawler.settings.get("MONGO_DB", "testing"),
            mongo_username=crawler.settings.get("MONGO_USERNAME", "admin"),
            mongo_password=crawler.settings.get("MONGO_PASSWORD", "test"),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(
            host=self.mongo_host,
            username=self.mongo_username,
            password=self.mongo_password,
            authSource="admin",
        )
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item: dict, spider):
        ceref = item.get("ceref")
        if ceref is None:
            raise KeyError("No ceref in item")

        result = self.db[self.collection].update_one(
            {"ceref": item.get("ceref")},
            {"$set": ItemAdapter(item).asdict()},
            upsert=True,
        )
        spider.log(f"{result.matched_count=}")
        spider.log(f"{result.modified_count=}")
        return item
