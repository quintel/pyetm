"""Service for fetching user scenarios (ETEngine sessions)."""

from typing import Any, Dict, List, Optional

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class FetchUserScenariosRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for fetching all scenarios belonging to the authenticated user.

    GET /api/v3/scenarios

    Supports pagination via query parameters:
    - page: Page number (1-indexed) - if provided, fetches only that page
    - per_page: Results per page (default 25)

    When page is not provided, automatically fetches all pages.
    """

    @staticmethod
    def run(
        client: BaseClient,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        **kwargs: Any,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Fetch scenarios for the authenticated user.

        Args:
            client: HTTP client with authentication
            page: Optional page number. If provided, fetches only that page.
                  If None, fetches all pages automatically.
            per_page: Optional number of results per page
            **kwargs: Additional query parameters

        Returns:
            ServiceResult with list of scenario data dictionaries
        """
        # If page is explicitly provided, fetch only that page
        if page is not None:
            return FetchUserScenariosRunner._fetch_single_page(
                client, page, per_page, kwargs
            )

        # Otherwise, fetch all pages automatically
        return FetchUserScenariosRunner._fetch_all_pages(client, per_page, kwargs)

    @staticmethod
    def _fetch_single_page(
        client: BaseClient,
        page: int,
        per_page: Optional[int],
        extra_params: Dict[str, Any],
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Fetch a single page of results."""
        params: Dict[str, Any] = {"page": page}

        if per_page is not None:
            params["per_page"] = per_page

        params.update(extra_params)

        result = FetchUserScenariosRunner._make_request(
            client=client,
            method="get",
            path="/scenarios",
            payload=params,
        )

        if not result.success:
            return result

        if result.data is None:
            return ServiceResult.fail(["No data returned from API"])

        # Extract data from paginated or direct response
        return FetchUserScenariosRunner._extract_data(result.data)

    @staticmethod
    def _fetch_all_pages(
        client: BaseClient,
        per_page: Optional[int],
        extra_params: Dict[str, Any],
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Fetch all pages by looping until empty page."""
        all_data: List[Dict[str, Any]] = []
        current_page = 1

        # Build params for first request
        params: Dict[str, Any] = {"page": 1}
        if per_page is not None:
            params["per_page"] = per_page
        params.update(extra_params)

        # Make first request to check if API is paginated
        first_result = FetchUserScenariosRunner._make_request(
            client=client,
            method="get",
            path="/scenarios",
            payload=params,
        )

        if not first_result.success:
            return first_result

        if first_result.data is None:
            return ServiceResult.fail(["No data returned from API"])

        # If response is a direct list (not paginated), return it immediately
        if isinstance(first_result.data, list):
            return ServiceResult.ok(data=first_result.data)

        # Otherwise, it's paginated - extract first page data
        extract_result = FetchUserScenariosRunner._extract_data(first_result.data)
        if not extract_result.success:
            return extract_result

        # If first page is empty, return immediately
        if not extract_result.data:
            return ServiceResult.ok(data=[])

        all_data.extend(extract_result.data)
        current_page = 2

        # Continue fetching remaining pages
        while True:
            page_result = FetchUserScenariosRunner._fetch_single_page(
                client, current_page, per_page, extra_params
            )

            if not page_result.success:
                # If we already got some data, return it with warnings
                if all_data:
                    return ServiceResult.ok(
                        data=all_data,
                        errors=[
                            f"Stopped at page {current_page} due to error: {'; '.join(page_result.errors)}"
                        ],
                    )
                # Otherwise propagate the error
                return page_result

            # Empty page means we're done
            if not page_result.data:
                break

            all_data.extend(page_result.data)
            current_page += 1

        return ServiceResult.ok(data=all_data)

    @staticmethod
    def _extract_data(response_data: Any) -> ServiceResult[List[Dict[str, Any]]]:
        """Extract data list from paginated or direct response."""
        # Handle paginated response with 'data' key
        if isinstance(response_data, dict) and "data" in response_data:
            data = response_data.get("data", [])
            if not isinstance(data, list):
                return ServiceResult.fail(
                    [f"Expected 'data' key to contain list, got {type(data).__name__}"]
                )
            return ServiceResult.ok(data=data)

        # Handle direct list response (backwards compatibility)
        if isinstance(response_data, list):
            return ServiceResult.ok(data=response_data)

        # Invalid response type
        return ServiceResult.fail(
            [
                f"Expected list or paginated response, got {type(response_data).__name__}"
            ]
        )
