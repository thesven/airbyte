from typing import Any, Mapping, MutableMapping

import requests

from partners_api import ShopifyPartnersAPI
from source import ShopifyPartnersStream


class ShopifyPartnersRelationshipStream(ShopifyPartnersStream):
    """
    To be used as a base class for all Relationship Streams
    """

    url_base = ""
    application_id = ""
    api_key = ""
    api_version = ""

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)

    def parse_response(self, response: requests.Response, **kwargs):
        response_data = response.json()

        # Iterate over each event in the response and yield it
        for event in response_data["data"]["app"]["events"]["edges"]:
            yield {
                "event_type": event["node"]["type"],  # Adjusted key to match schema
                "occurredAt": event["node"]["occurredAt"],
                "app_id": event["node"]["app"]["id"],
                "app_name": event["node"]["app"]["name"],
                "shop_id": event["node"]["shop"]["id"],
                "shop_name": event["node"]["shop"]["name"],
            }


class RelationshipInstalls(ShopifyPartnersRelationshipStream):
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
        q, v = api.get_events_installs(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class RelationshipUninstalls(ShopifyPartnersRelationshipStream):
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
        q, v = api.get_events_uninstalls(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class RelationshipReactivated(ShopifyPartnersRelationshipStream):
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
        q, v = api.get_events_reactivations(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class RelationshipDeactivated(ShopifyPartnersRelationshipStream):
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
        q, v = api.get_events_deactivations(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}
