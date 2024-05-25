import requests


class FindWorkApiClient:
    def __init__(self, api_key: str):
        self.base_url = "https://findwork.dev/api"
        if api_key is None:
            raise ValueError("API key cannot be set to None.")
        self.api_key = api_key

    def get_jobs(self, search_query: str, location: str = None, page: int = 1) -> dict:
        """
        Get job listings based on the search query and location.

        Args:
            search_query: Keywords to search for in job titles and descriptions.
            location: Location filter for job search.
            page: Page number of job listings to retrieve (default is 1).

        Returns:
            Dictionary containing job listings.

        Raises:
            Exception if response code is not 200.
        """
        params = {}
        # params = {"search": search_query, "page": page}
        if location:
            params["location"] = location
        if search_query:
            params["search"] = search_query
        if page:
            params["page"] = page
        print(params)
        headers = {"Authorization": f"Token {self.api_key}"}
        response = requests.get(
            f"{self.base_url}/jobs/", params=params, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to fetch job listings from Findwork API. Status Code: {response.status_code}. Response: {response.text}"
            )
