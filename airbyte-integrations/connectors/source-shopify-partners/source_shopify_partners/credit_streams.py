from typing import Mapping, Any, MutableMapping

import requests

from partners_api import ShopifyPartnersAPI
from source import ShopifyPartnersStream


class ShopifyCreditStream(ShopifyPartnersStream):
    """
    To be used as a base class for all Credit Streams
    """

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)

    def parse_response(self, response: requests.Response, **kwargs):
        response_data = response.json()

        # Iterate over each event in the response and yield it
        for event in response_data["data"]["app"]["events"]["edges"]:
            yield {
                "type": event["node"]["type"],
                "occurredAt": event["node"]["occurredAt"],
                "app_id": event["node"]["app"]["id"],
                "app_name": event["node"]["app"]["name"],
                "shop_id": event["node"]["shop"]["id"],
                "shop_name": event["node"]["shop"]["name"],
                "app_credit_id": event["node"]["charge"]["id"],
                "app_credit_test": event["node"]["charge"]["test"],
                "app_credit_name": event["node"]["charge"]["name"],
                "app_credit_amount": event["node"]["charge"]["amount"]["amount"],
                "app_credit_currency_code": event["node"]["charge"]["amount"][
                    "currencyCode"
                ],
            }


class CreditApplied(ShopifyCreditStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.config = config

    def request_body_json(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        api = ShopifyPartnersAPI(self.api_key, self.api_version)
        q, v = api.get_events_credit_applied(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class CreditPending(ShopifyCreditStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.config = config

    def request_body_json(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        api = ShopifyPartnersAPI(self.api_key, self.api_version)
        q, v = api.get_events_credit_pending(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class CreditFailed(ShopifyCreditStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.config = config

    def request_body_json(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        api = ShopifyPartnersAPI(self.api_key, self.api_version)
        q, v = api.get_events_credit_failed(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}
