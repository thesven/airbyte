#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from typing import Any, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

from credit_streams import CreditFailed, CreditApplied, CreditPending
from partners_api import ShopifyPartnersAPI
from releationship_streams import (
    RelationshipInstalls,
    RelationshipUninstalls,
    RelationshipDeactivated,
    RelationshipReactivated,
)


# Basic full refresh stream
class ShopifyPartnersStream(HttpStream, ABC):
    """
    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class ShopifyPartnersStream(HttpStream, ABC)` which is the current class
    `class Customers(ShopifyPartnersStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(ShopifyPartnersStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalShopifyPartnersStream((ShopifyPartnersStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    url_base = "https://partners.shopify.com"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.url_base = f"https://partners.shopify.com/{config['partner_id']}/api/{config['api_version']}/graphql.json"
        self.application_id = f"gid://partners/App/{config['application_id']}"
        self.api_key = config["api_key"]
        self.api_version = config["api_version"]

    def _create_prepared_request(
            self,
            path: str,
            headers: Optional[Mapping[str, str]] = None,
            params: Optional[Mapping[str, str]] = None,
            json: Optional[Mapping[str, Any]] = None,
            data: Optional[Union[str, Mapping[str, Any]]] = None,
    ) -> requests.PreparedRequest:
        url = self._join_url(self.url_base, path)
        if self.must_deduplicate_query_params():
            query_params = self.deduplicate_query_params(url, params)
        else:
            query_params = params or {}
        args = {
            "method": "POST",
            "url": url,
            "headers": headers,
            "params": query_params,
        }
        if json and data:
            raise Exception(
                "At the same time only one of the 'request_body_data' and 'request_body_json' functions can return data"
            )
        elif json:
            args["json"] = json
        elif data:
            args["data"] = data

        prepared_request: requests.PreparedRequest = self._session.prepare_request(
            requests.Request(**args)
        )
        return prepared_request

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        # we don't need to append anything for the path as it is a single endpoint we are dealing with
        return ""

    def request_headers(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {
            "X-Shopify-Access-Token": self.api_key,
            "Content-Type": "application/json",
        }

    def next_page_token(
            self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        response_data = response.json()
        if response_data["data"]["app"]["events"]["pageInfo"]["hasNextPage"] == True:
            print(
                "NEXT PAGE TOKEN :",
                response_data["data"]["app"]["events"]["edges"][-1]["cursor"],
            )
            return response_data["data"]["app"]["events"]["edges"][-1]["cursor"]

        return None

    def http_method(self) -> str:
        """
        Override if needed. See get_request_data/get_request_json if using POST/PUT/PATCH.
        """
        return "POST"


class ShopifySubscriptionWithCostStream(ShopifyPartnersStream):
    """
    To be used as a base class for all Subscription Streams w/ cost
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
                "app": {
                    "id": event["node"]["app"]["id"],
                    "name": event["node"]["app"]["name"],  # Corrected from id to name
                },
                "shop": {
                    "id": event["node"]["shop"]["id"],
                    "name": event["node"]["shop"]["name"],  # Corrected from id to name
                },
                "charge": {
                    "id": event["node"]["charge"]["id"],
                    "test": event["node"]["charge"]["test"],
                    "name": event["node"]["charge"]["name"],
                    "billingOn": event["node"]["charge"]["billingOn"],
                    "amount": event["node"]["charge"]["amount"],
                },
            }


class SubscriptionCappedAmountUpdated(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_capped_amount_updated(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionApproachingCappedAmount(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_approaching_capped_amount(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionChargeAccepted(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_charge_accepted(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionChargeActivated(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_charge_activated(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionChargeCanceled(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_charge_canceled(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionChargeDeclined(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_charge_declined(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionChargeExpired(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_charge_expired(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionChargeFrozen(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_charge_frozen(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class SubscriptionChargeUnfrozen(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_subscription_charge_unfrozen(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class UsageChargeApplied(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_usage_charge_applied(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class OneTimeChargeAccepted(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_one_time_charge_accepted(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class OneTimeChargeExpired(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_one_time_charge_expired(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class OneTimeChargeActivated(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_one_time_charge_activated(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


class OneTimeChargeDeclined(ShopifySubscriptionWithCostStream):
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
        q, v = api.get_events_one_time_charge_declined(
            self.application_id,
            int(self.config["num_results_per_call"]),
            next_page_token,
        )
        return {"query": q, "variables": v}


# Basic incremental stream
class IncrementalShopifyPartnersStream(ShopifyPartnersStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(
            self,
            current_stream_state: MutableMapping[str, Any],
            latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


# Source
class SourceShopifyPartners(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        # # check to see that the config values are appropriate
        api_key = config["api_key"]
        api_version = config["api_version"]
        partner_id = config["partner_id"]
        application_id = f"gid://partners/App/{config['application_id']}"

        try:
            # attempt to make a call to the endpoint
            request_url = f"https://partners.shopify.com/{partner_id}/api/{api_version}/graphql.json"
            headers = {
                "X-Shopify-Access-Token": api_key,
                "Content-Type": "application/json",
            }
            api = ShopifyPartnersAPI(api_key, partner_id, api_version)
            q, v = api.get_events_installs(
                application_id, int(config["num_results_per_call"])
            )
            data = {"query": q, "variables": v}
            response = requests.post(url=request_url, headers=headers, json=data)
            results = response.json()

            if "error" in results.keys():
                return False, f"{results['error']}"

        except (
                requests.exceptions.RequestException,
                requests.exceptions.HTTPError,
        ) as e:
            return False, f"Unable to connect to the shopify partners api :: {e}"

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = NoAuth()
        return [
            RelationshipInstalls(authenticator=auth, config=config),
            RelationshipUninstalls(authenticator=auth, config=config),
            RelationshipReactivated(authenticator=auth, config=config),
            RelationshipDeactivated(authenticator=auth, config=config),
            # SubscriptionCappedAmountUpdated(authenticator=auth, config=config),
            # SubscriptionApproachingCappedAmount(authenticator=auth, config=config),
            # SubscriptionChargeAccepted(authenticator=auth, config=config),
            # SubscriptionChargeActivated(authenticator=auth, config=config),
            # SubscriptionChargeCanceled(authenticator=auth, config=config),
            # SubscriptionChargeDeclined(authenticator=auth, config=config),
            # SubscriptionChargeExpired(authenticator=auth, config=config),
            # SubscriptionChargeFrozen(authenticator=auth, config=config),
            # SubscriptionChargeUnfrozen(authenticator=auth, config=config),
            # UsageChargeApplied(authenticator=auth, config=config),
            CreditApplied(authenticator=auth, config=config),
            CreditPending(authenticator=auth, config=config),
            CreditFailed(authenticator=auth, config=config),
            # OneTimeChargeAccepted(authenticator=auth, config=config),
            # OneTimeChargeActivated(authenticator=auth, config=config),
            # OneTimeChargeDeclined(authenticator=auth, config=config),
            # OneTimeChargeExpired(authenticator=auth, config=config),
        ]
