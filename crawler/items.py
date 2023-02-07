# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class SFCItem(Item):
    ceref = Field()
    name = Field()
    nameChi = Field()
    tel = Field()
    email = Field()
    conditions = Field()
    disciplinary_actions = Field()


class WebbFirmItem(Item):
    ceref = Field()
    name = Field()
    num_ros = Field()
    num_reps = Field()
    licensed_on = Field()
    webb_code = Field()
    domicile = Field()
    formed_on = Field()
    incorporation_number = Field()
    website = Field()

    hist_num_professionals = Field()

    licensees = Field()
