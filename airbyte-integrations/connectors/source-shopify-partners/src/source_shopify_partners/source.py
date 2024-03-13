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


class ShopifyPartnersAPI:
    def __init__(
            self,
            partners_api_key: str,
            partners_organization_id: str,
            partners_api_version: str = "2024-01",
    ):
        self.partners_api_key = partners_api_key
        self.partners_organization_id = partners_organization_id
        self.partners_api_version = partners_api_version

    def _compose_query(
            self, app_id: str, event_types: list, first: int = 10, after: str = None
    ):
        types_fragment = "[" + ", ".join(f"{type_}" for type_ in event_types) + "]"

        query = f"""
        query GetAppEvents($appId: ID!, $first: Int!, $after: String) {{
          app(id: $appId) {{
            events(first: $first, after: $after, types: {types_fragment}) {{
              pageInfo {{
                hasNextPage
                hasPreviousPage
              }}
              edges {{
                cursor
                node {{
                  ...EventDetails
                }}
              }}
            }}
          }}
        }}

        fragment EventDetails on AppEvent {{
          type
          occurredAt
          app {{
            ...AppInfo
          }}
          shop {{
            ...ShopInfo
          }}
          ... on AppSubscriptionEvent {{
          charge {{
            ...ChargeInfo
            }}
          }}
          ... on AppCreditEvent {{
            appCredit {{
            ...AppCredit
            }}
          }}
        }}

        fragment AppInfo on App {{
          id
          name
        }}

        fragment ShopInfo on Shop {{
          id
          name
        }}

        fragment AppCredit on AppCredit {{
            id
            name
            test
        }}

        fragment ChargeInfo on AppSubscription {{
            id
            test
            name
            billingOn
            amount {{
                amount
                currencyCode
            }}

        }}
        """
        variables = {"appId": app_id, "first": first, "after": after}
        return query, variables

    # Adding all original methods, specifying whether to include charge information
    def get_events_installs(self, app_id: str, first: int = 10, after: str = None):
        event_types = ["RELATIONSHIP_INSTALLED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_uninstalls(self, app_id: str, first: int = 10, after: str = None):
        event_types = ["RELATIONSHIP_UNINSTALLED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_deactivations(self, app_id: str, first: int = 10, after: str = None):
        event_types = ["RELATIONSHIP_DEACTIVATED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_reactivations(self, app_id: str, first: int = 10, after: str = None):
        event_types = ["RELATIONSHIP_REACTIVATED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_approaching_capped_amount(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_APPROACHING_CAPPED_AMOUNT"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_capped_amount_updated(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CAPPED_AMOUNT_UPDATED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_charge_accepted(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CHARGE_ACCEPTED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_charge_activated(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CHARGE_ACTIVATED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_charge_canceled(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CHARGE_CANCELED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_charge_declined(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CHARGE_DECLINED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_charge_expired(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CHARGE_EXPIRED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_charge_frozen(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CHARGE_FROZEN"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_subscription_charge_unfrozen(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["SUBSCRIPTION_CHARGE_UNFROZEN"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_credit_applied(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["CREDIT_APPLIED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_credit_failed(self, app_id: str, first: int = 10, after: str = None):
        event_types = ["CREDIT_FAILED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_credit_pending(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["CREDIT_PENDING"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_one_time_charge_accepted(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["ONE_TIME_CHARGE_ACCEPTED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_one_time_charge_activated(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["ONE_TIME_CHARGE_ACTIVATED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_one_time_charge_declined(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["ONE_TIME_CHARGE_DECLINED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_one_time_charge_expired(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["ONE_TIME_CHARGE_EXPIRED"]
        return self._compose_query(app_id, event_types, first, after)

    def get_events_usage_charge_applied(
            self, app_id: str, first: int = 10, after: str = None
    ):
        event_types = ["USAGE_CHARGE_APPLIED"]
        return self._compose_query(app_id, event_types, first, after)


"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


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

    def next_page_token(
            self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

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


class ShopifyPartnersRelationshipStream(ShopifyPartnersStream):
    """
    To be used as a base class for all Relationship Streams
    """

    url_base = ""
    application_id = ""
    api_key = ""
    api_version = ""

    primary_key = "occurredAt"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.url_base = f"https://partners.shopify.com/{config['partner_id']}/api/{config['api_version']}/graphql.json"
        self.application_id = f"gid://partners/App/{config['application_id']}"
        self.api_key = config["api_key"]
        self.api_version = config["api_version"]

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
            }


class ShopifySubscriptionWithCostStream(ShopifyPartnersStream):
    """
    To be used as a base class for all Subscription Streams w/ cost
    """

    url_base = ""
    application_id = ""
    api_key = ""
    api_version = ""

    primary_key = "occurredAt"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.url_base = f"https://partners.shopify.com/{config['partner_id']}/api/{config['api_version']}/graphql.json"
        self.application_id = f"gid://partners/App/{config['application_id']}"
        self.api_key = config["api_key"]
        self.api_version = config["api_version"]

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
        q, v = api.get_events_subscription_charge_activated(
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
            q, v = api.get_events_installs(application_id, 1)
            data = {"query": q, "variables": v}
            response = requests.post(url=request_url, headers=headers, json=data)
            results = response.json()

            print("RESULTS", results)
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
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = NoAuth()
        return [
            RelationshipInstalls(authenticator=auth, config=config),
            RelationshipUninstalls(authenticator=auth, config=config),
            RelationshipReactivated(authenticator=auth, config=config),
            RelationshipDeactivated(authenticator=auth, config=config),
            SubscriptionChargeAccepted(authenticator=auth, config=config),
        ]
