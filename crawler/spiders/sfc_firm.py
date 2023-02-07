import json
import string
from datetime import datetime

import scrapy
from scrapy.http import FormRequest

from ..items import SFCItem

"""
/co: compliance tel: cofficerData
/conditions: condData
/da: disRemarkData
DocSeq: https://apps.sfc.hk/publicregWeb/displayFile?docno={DocSeq}
or: http://www.sfc.hk//publicregWeb/displayFile?docno={DocSeq}
"""

general_pattern = r"\bvar\s+\w+Data\s*=\s*(\[.*?\])\s*;\s*\n"
email_pattern = r"\bvar\s+emailData\s*=\s*(\[.*?\])\s*;\s*\n"

PARAM_MAP = {
    "email": {
        "route": "/addresses",
        "pattern": email_pattern,
        "fields": ["email"],
        "is_multiple": False,
    },
    "tel": {
        "route": "/co",
        "pattern": general_pattern,
        "fields": ["tel"],
        "is_multiple": False,
    },
    "conditions": {
        "route": "/conditions",
        "pattern": general_pattern,
        "fields": ["conditionDtl", "conditionCDtl", "effDate"],
        "is_multiple": True,
    },
    "disciplinary_actions": {
        "route": "/da",
        "pattern": general_pattern,
        "fields": ["actnDate", "codeDesc", "codeCdesc", "engDocSeq", "chiDocSeq"],
        "is_multiple": True,
    },
}


def get_form_data(start_letter):
    # unused params: start, page
    return {
        "licstatus": "active",
        "ratype": "6",
        "roleType": "corporation",
        "limit": "200",  # may hit upper bound
        "nameStartLetter": start_letter,
    }


class SFCFirmSpider(scrapy.Spider):
    name = "sfc_firm"
    allowed_domains = ["www.sfc.com", "apps.sfc.hk"]
    # start_urls = [""]

    custom_settings = {
        "ITEM_PIPELINES": {
            "crawler.pipelines.FilterSFCFirmPipeline": 100,
            "crawler.pipelines.MongoFirmPipeline": 200,
        },
        "LOG_FILE": f"{datetime.now().date()}_scrapy_{name}.log",
    }

    def start_requests(self):
        for letter in string.ascii_uppercase:
            yield FormRequest(
                "https://apps.sfc.hk/publicregWeb/searchByRaJson",
                formdata=get_form_data(letter),
                callback=self.parse_item_list_with_name_start_letter,
            )

    def parse_item_list_with_name_start_letter(self, response):
        def _get_res_item_type(res_item):
            if res_item.get("isCorp"):
                company_type = "corp"
            elif res_item.get("isRi"):
                company_type = "ri"
            elif res_item.get("isEo"):
                company_type = "eo"
            elif res_item.get("isIndi"):
                company_type = "indi"
            else:
                raise ValueError("Cannot determine company_type")

            return company_type

        result = response.json()
        total_count: int = result.get("totalCount")

        # there might be no companies with a specific start letter, e.g. 'Q'
        if total_count == 0:
            return

        res_items: list = result.get("items")
        start_letter = res_items[0].get("name")[0]

        # check server count and items received to avoid unexpected result
        if (count := len(res_items)) != total_count:
            raise ValueError(
                f"Mismatch item count with letter {start_letter}: {count}/{total_count}"
            )

        # begins
        for res_item in res_items:
            ceref = res_item.get("ceref")
            item = SFCItem()
            item["ceref"] = ceref
            item["name"] = res_item.get("name")
            item["nameChi"] = res_item.get("nameChi")

            company_type = _get_res_item_type(res_item)

            fields = PARAM_MAP.keys()
            for field in fields:
                yield response.follow(
                    self.get_url(ceref, company_type, field),
                    callback=self.parse_item_field,
                    cb_kwargs={"item": item, "field": field},
                )

    def get_url(self, ceref, company_type, field):
        route = PARAM_MAP[field].get("route")
        return f"https://apps.sfc.hk/publicregWeb/{company_type}/{ceref}{route}"

    def parse_item_field(self, response, field, item):
        def _json_str_to_json(response, pattern):
            json_str = response.css("script::text").re_first(pattern)

            if json_str is None:
                raise ValueError("result not found")

            json_data = json.loads(json_str)
            return json_data

        def _json_to_field_val(json_data, fields, is_multiple):
            if len(json_data) == 0:
                if is_multiple:
                    return []
                else:
                    return ""

            match (len(json_data) == 1, len(fields) == 1, is_multiple):
                case (True, True, False):
                    return json_data[0].get(fields[0])
                case (_, False, True):
                    return _get_item_field_with_sub_fields(json_data, fields)
                case (False, True, False):
                    # FIXME: Temp fix for the case that
                    # multiple records exist, but only one is needed
                    # e.g. https://apps.sfc.hk/publicregWeb/ri/AAN851/co
                    # FIXME: find "Was out of use case" in log and fix those records
                    return json_data[0].get(fields[0])
                case _:
                    raise NotImplementedError("Was out of use case")

        def _get_item_field_with_sub_fields(json_data, fields):
            values = []
            for json_datum in json_data:
                value = {}
                for field in fields:
                    value[field] = json_datum.get(field)
                values.append(value)
            return values

        json_data = _json_str_to_json(response, PARAM_MAP[field].get("pattern"))

        item[field] = _json_to_field_val(
            json_data,
            PARAM_MAP[field].get("fields"),
            PARAM_MAP[field].get("is_multiple"),
        )
        yield item
