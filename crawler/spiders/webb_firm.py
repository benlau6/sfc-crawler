import json
import string
from datetime import datetime

import scrapy

from ..items import WebbFirmItem

"""
/co: compliance tel: cofficerData
/conditions: condData
/da: disRemarkData
DocSeq: https://apps.sfc.hk/publicregWeb/displayFile?docno={DocSeq}
or: http://www.sfc.hk//publicregWeb/displayFile?docno={DocSeq}
"""

general_pattern = r"\bvar\s+\w+Data\s*=\s*(\[.*?\])\s*;\s*\n"
email_pattern = r"\bvar\s+emailData\s*=\s*(\[.*?\])\s*;\s*\n"


class WebbFirmSpider(scrapy.Spider):
    name = "webb_firm"
    allowed_domains = ["webb-site.com"]
    base_url = "https://webb-site.com/dbpub"
    # start_urls = [""]

    custom_settings = {
        "ITEM_PIPELINES": {
            "crawler.pipelines.MongoFirmPipeline": 200,
        },
        "LOG_FILE": f"{datetime.now().date()}_scrapy_{name}.log",
    }

    def start_requests(self):
        yield scrapy.Request(
            "https://webb-site.com/dbpub/SFClicount.asp?s=cntdn&a=6",
            callback=self.parse,
        )

    def parse(self, response):
        trs = response.xpath("//table[@class='numtable']/tr")

        headers = trs[0].xpath("th//text()").extract()
        if headers != [
            "Row",
            "Name",
            "ROs",
            "Reps",
            "Total",
            "Reps v",
            "total %",
            "Licensed",
        ]:
            raise ValueError(
                f"Table headers were changed, please review {response.url}"
            )

        for tr in trs[1:-1]:
            # skip summary and header
            item = WebbFirmItem()
            item["name"] = tr.xpath("td[2]//text()").extract_first()
            item["num_ros"] = tr.xpath("td[3]//text()").extract_first()
            item["num_reps"] = tr.xpath("td[4]//text()").extract_first()

            licensed_on = tr.xpath("td[7]//text()").extract_first()
            if licensed_on is None:
                licensed_on = "2003-04-01"
            item["licensed_on"] = datetime.strptime(licensed_on, "%Y-%m-%d").date()

            webb_code = tr.xpath("td[@class='left']/a/@href").re_first(r"p=(\d+)&")
            item["webb_code"] = webb_code

            orgdata_url = f"{self.base_url}/orgdata.asp?p={webb_code}"
            yield response.follow(
                orgdata_url, callback=self.parse_orgdata, cb_kwargs={"item": item}
            )

            # histfirm_url = f"{self.base_url}/SFChistfirm.asp?p={webb_code}&a=6"
            # yield response.follow(
            #     histfirm_url,
            #     callback=self.parse_hist_num_licensees,
            #     cb_kwargs={"item": item},
            # )

            # # TODO: + &h=N can show historical records for licensees
            # # But it shd rather be done in SFC side in view of individual
            # # it shows historical data but it will lead to duplicate name
            # licensees_url = f"{self.base_url}/SFClicensees.asp?p={webb_code}&h=Y&a=6"
            # yield response.follow(
            #     licensees_url,
            #     callback=self.parse_licensees,
            #     cb_kwargs={"item": item},
            # )

    def parse_orgdata(self, response, item):
        trs = response.xpath("//table/tr")
        for tr in trs:
            if tr.xpath("td//text()").re_first("Domicile:"):
                item["domicile"] = tr.xpath("td[2]//text()").extract_first()
            elif tr.xpath("td//text()").re_first("Incorporation number:"):
                item["incorporation_number"] = tr.xpath(
                    "td[2]/a/text()"
                ).extract_first()
            elif tr.xpath("td//text()").re_first("Formed:"):
                formed_on = tr.xpath("td[2]//text()").extract_first()
                if formed_on is not None:
                    item["formed_on"] = datetime.strptime(formed_on, "%Y-%m-%d").date()
            elif tr.xpath("td//text()").re_first("SFC ID:"):
                item["ceref"] = tr.xpath("td[2]/a/text()").extract_first()
            elif tr.xpath("td//text()").re_first("Web sites:"):
                item["website"] = tr.xpath("td[2]/a/@href").extract_first()
        yield item

    def parse_hist_num_licensees(self, response, item):
        trs = response.xpath("//table[@class='numtable center']/tr")
        headers = trs[0].xpath("th//text()").extract()
        if headers != ["Date", "ROs", "Reps", "Total", "Reps v total"]:
            raise ValueError(f"Table headers changed, please review {response.url}")

        arr = []
        for tr in trs[1:]:
            obj = {}
            obj["date"] = datetime.strptime(
                tr.xpath("td[1]//text()").extract_first(), "%Y-%m-%d"
            ).date()
            obj["num_ros"] = tr.xpath("td[2]//text()").extract_first()
            obj["num_reps"] = tr.xpath("td[3]//text()").extract_first()
            arr.append(obj)
        item["hist_num_professionals"] = arr
        yield item

    def parse_licensees(self, response, item):
        trs = response.xpath("//table[@class='opltable']/tr")
        headers = trs[0].xpath("th//text()").extract()
        if headers != ["Name", "Age in", "2022", "⚥", "Role", "From", "Until"]:
            raise ValueError(f"Table headers changed, please review {response.url}")

        arr = []
        for tr in trs[1:]:
            obj = {}
            obj["name"] = tr.xpath("td[2]//text()").extract_first()
            obj["role"] = tr.xpath("td[5]//text()").extract_first()
            employed_from = tr.xpath(
                "td[@class='colHide3 nowrap']/text()"
            ).extract_first()
            if employed_from is not None:
                obj["employed_from"] = datetime.strptime(
                    employed_from, "%Y-%m-%d"
                ).date()

            arr.append(obj)
        item["licensees"] = arr
        yield item
