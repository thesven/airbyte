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
