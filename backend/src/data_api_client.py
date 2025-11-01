import hashlib
import json
import logging
import time
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from requests import Response
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential, retry_if_exception_type


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataGovAPIClient:
    """Client for fetching datasets from data.gov.in.

    Features:
    - API filter construction via filters[Column] and range operators
    - Pagination using offset/limit
    - Response caching with a 1-hour TTL
    - Robust retries with exponential backoff for transient errors

    Parameters
    ----------
    api_key: str
        data.gov.in API key.
    base_url: str
        Base endpoint. Example: "https://api.data.gov.in/resource/".
    cache_ttl_seconds: int, optional
        Cache time-to-live in seconds. Default 3600s (1 hour).
    session: requests.Session, optional
        Optional shared session for connection pooling.
    """

    # Resource IDs
    CROP_PRODUCTION_RESOURCE_ID = "35be999b-0208-4354-b557-f6ca9a5355de"
    RAINFALL_RESOURCE_ID = "440dbca7-86ce-4bf6-b1af-83af2855757e"

    # Subdivision to State mapping
    SUBDIVISION_TO_STATES: Mapping[str, List[str]] = {
        "Arunachal Pradesh": ["Arunachal Pradesh"],
        "Assam & Meghalaya": ["Assam", "Meghalaya"],
        "NMMT": ["Nagaland", "Manipur", "Mizoram", "Tripura"],
        "Sub-Himalayan West Bengal & Sikkim": ["West Bengal", "Sikkim"],
        "Gangetic West Bengal": ["West Bengal"],
        "Orissa": ["Odisha"],
        "Jharkhand": ["Jharkhand"],
        "Bihar": ["Bihar"],
        "East Uttar Pradesh": ["Uttar Pradesh"],
        "West Uttar Pradesh": ["Uttar Pradesh"],
        "Uttarakhand": ["Uttarakhand"],
        "Haryana Delhi & Chandigarh": ["Haryana", "Delhi", "Chandigarh"],
        "Punjab": ["Punjab"],
        "Himachal Pradesh": ["Himachal Pradesh"],
        "Jammu & Kashmir": ["Jammu and Kashmir"],
        "West Rajasthan": ["Rajasthan"],
        "East Rajasthan": ["Rajasthan"],
        "West Madhya Pradesh": ["Madhya Pradesh"],
        "East Madhya Pradesh": ["Madhya Pradesh"],
        "Gujarat Region": ["Gujarat"],
        "Saurashtra & Kutch": ["Gujarat"],
        "Konkan & Goa": ["Maharashtra", "Goa"],
        "Madhya Maharashtra": ["Maharashtra"],
        "Marathwada": ["Maharashtra"],
        "Vidarbha": ["Maharashtra"],
        "Chhattisgarh": ["Chhattisgarh"],
        "Coastal Andhra Pradesh": ["Andhra Pradesh"],
        "Telangana": ["Telangana"],
        "Rayalaseema": ["Andhra Pradesh"],
        "Tamil Nadu": ["Tamil Nadu"],
        "Coastal Karnataka": ["Karnataka"],
        "North Interior Karnataka": ["Karnataka"],
        "South Interior Karnataka": ["Karnataka"],
        "Kerala": ["Kerala"],
    }

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.data.gov.in/resource/",
        *,
        cache_ttl_seconds: int = 3600,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.cache_ttl_seconds = cache_ttl_seconds
        self.session = session or requests.Session()

        # in-memory cache: key -> (expires_at_epoch, data)
        self._cache: MutableMapping[str, Tuple[float, Any]] = {}

    # -------------------- Public API --------------------
    def fetch_crop_production(
        self, 
        state: str = None, 
        district: str = None,
        crop: str = None,
        year: int = None,
        season: str = None,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """Fetch crop production data"""
        
        resource_id = self.CROP_PRODUCTION_RESOURCE_ID
        url = f"{self.base_url}{resource_id}"
        
        params = {
            'api-key': self.api_key,
            'format': 'json',
            'limit': 100,
            'offset': 0
        }
        
        # Add filters only if provided
        if state:
            standardized_state = self.standardize_state_name(state)
            params['filters[state_name]'] = standardized_state
        if district:
            params['filters[district_name]'] = district
        if crop:
            params['filters[crop]'] = crop
        if year:
            params['filters[crop_year]'] = year
        if season:
            params['filters[season]'] = season
        
        print(f"\n{'='*50}")
        print(f"FETCH_CROP_PRODUCTION CALLED!")
        print(f"State: {state}, Year: {year}, Crop: {crop}")
        print(f"{'='*50}\n")
        
        try:
            all_records = []
            
            # For queries without year filter, limit to 2000 records max
            # (covers about 2-3 years of data per state)
            max_fetch = 2000 if not year else limit
            
            # Fetch with pagination
            while len(all_records) < max_fetch:
                print(f"Fetching page at offset {params['offset']}... (have {len(all_records)} so far)")
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                records = data.get('records', [])
                if not records:
                    print("No more records, breaking")
                    break
                
                all_records.extend(records)
                
                # If we got fewer records than requested, we're done
                if len(records) < params['limit']:
                    print(f"Got fewer records than limit ({len(records)} < {params['limit']}), breaking")
                    break
                
                # Move to next page
                params['offset'] += params['limit']
                
                # Safety: max 20 pages for unfiltered queries, 100 for filtered
                max_pages = 20 if not year else 100
                if params['offset'] >= (max_pages * params['limit']):
                    print(f"Hit max pages limit ({max_pages}), breaking")
                    break
            
            print(f"Final total: {len(all_records)} records fetched")
            return all_records
            
        except Exception as e:
            logger.error(f"Error fetching crop production: {e}")
            return []

    def fetch_rainfall_data(
        self,
        subdivision: str = None,
        year_start: int = None,
        year_end: int = None,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """Fetch rainfall data"""
        
        resource_id = self.RAINFALL_RESOURCE_ID
        url = f"{self.base_url}{resource_id}"
        
        params = {
            'api-key': self.api_key,
            'format': 'json',
            'limit': 1000,  # Get more records at once
            'offset': 0
        }
        
        # Only filter by subdivision, NOT by year (year filtering doesn't work properly)
        if subdivision:
            params['filters[subdivision]'] = subdivision.upper()
        
        print(f"\n{'='*50}")
        print(f"API REQUEST:")
        print(f"URL: {url}")
        print(f"Params: {params}")
        print(f"{'='*50}\n")
        
        try:
            all_records = []
            
            # Fetch with pagination
            while True:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                records = data.get('records', [])
                if not records:
                    break
                
                all_records.extend(records)
                
                # Check if we got all records
                total = int(data.get('total', 0))
                if len(all_records) >= total or len(records) < params['limit']:
                    break
                
                # Move to next page
                params['offset'] += params['limit']
                
                # Safety: max 10 pages
                if params['offset'] > 10000:
                    break
            
            print(f"Total rainfall records fetched: {len(all_records)}")
            
            # Filter by year range locally if specified
            if year_start or year_end:
                filtered = []
                for record in all_records:
                    try:
                        year = int(record.get('year', 0))
                        if year_start and year < year_start:
                            continue
                        if year_end and year > year_end:
                            continue
                        filtered.append(record)
                    except:
                        continue
                
                print(f"Filtered to {len(filtered)} records for years {year_start}-{year_end}")
                all_records = filtered
            
            # Compute annual if missing using monthly values average (simple mean)
            # Try lowercase first, fallback to uppercase for compatibility
            for rec in all_records:
                annual = rec.get("annual") or rec.get("ANNUAL")
                if annual is None or annual == "":
                    months = [
                        rec.get("jan") or rec.get("JAN"),
                        rec.get("feb") or rec.get("FEB"),
                        rec.get("mar") or rec.get("MAR"),
                        rec.get("apr") or rec.get("APR"),
                        rec.get("may") or rec.get("MAY"),
                        rec.get("jun") or rec.get("JUN"),
                        rec.get("jul") or rec.get("JUL"),
                        rec.get("aug") or rec.get("AUG"),
                        rec.get("sep") or rec.get("SEP"),
                        rec.get("oct") or rec.get("OCT"),
                        rec.get("nov") or rec.get("NOV"),
                        rec.get("dec") or rec.get("DEC"),
                    ]
                    numeric = [float(m) for m in months if _is_number(m)]
                    if numeric:
                        # Set in lowercase to match API format
                        rec["annual"] = sum(numeric) / len(numeric)
            
            return all_records[:limit] if limit else all_records
            
        except Exception as e:
            logger.error(f"Error fetching rainfall: {e}")
            return []

    # -------------------- Helpers --------------------
    def _build_filters(self, params: Mapping[str, Any]) -> Dict[str, Any]:
        """Translate a dict of filters into data.gov.in filter parameters.

        Examples
        --------
        {"state_name": "Maharashtra", "crop_year": 2020}
        -> {"filters[state_name]": "Maharashtra", "filters[crop_year]": 2020}

        {"year": {">=": 2019, "<=": 2021}}
        -> {"filters[year][>=]": 2019, "filters[year][<=]": 2021}
        """
        out: Dict[str, Any] = {
            "api-key": self.api_key,
            "format": "json",
        }

        for key, value in params.items():
            if isinstance(value, Mapping):
                for op, val in value.items():
                    out[f"filters[{key}][{op}]"] = val
            elif value is not None:
                out[f"filters[{key}]"] = value
        return out

    def _fetch_with_pagination(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 1000,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Fetch records using offset/limit pagination.

        Parameters
        ----------
        url: str
            Resource URL.
        params: Optional[Dict[str, Any]]
            Base params including filters, api-key, format.
        limit: int
            Maximum total records to fetch.
        page_size: int
            Number of records per request.
        """
        if params is None:
            params = {}

        results: List[Dict[str, Any]] = []
        offset = 0

        while len(results) < limit:
            remaining = limit - len(results)
            batch_size = min(page_size, remaining)
            page_params = dict(params)
            page_params.update({"limit": batch_size, "offset": offset})

            payload = self._get_with_cache(url, page_params)

            # data.gov.in returns 'records' or may use 'data' for some APIs; primary is 'records'.
            records: List[Dict[str, Any]] = payload.get("records") or payload.get("data") or []
            if not records:
                break

            results.extend(records)
            if len(records) < batch_size:
                break
            offset += batch_size

        return results[:limit]

    # -------------------- HTTP layer, caching, and retries --------------------
    def _cache_key(self, url: str, params: Mapping[str, Any]) -> str:
        key_material = json.dumps([url, sorted(params.items())], separators=(",", ":"))
        return hashlib.sha256(key_material.encode("utf-8")).hexdigest()

    def _get_with_cache(self, url: str, params: Mapping[str, Any]) -> Dict[str, Any]:
        now = time.time()
        key = self._cache_key(url, params)
        cached = self._cache.get(key)
        if cached and cached[0] > now:
            return cached[1]

        payload = self._get(url, params)
        self._cache[key] = (now + self.cache_ttl_seconds, payload)
        return payload

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
    )
    def _get(self, url: str, params: Mapping[str, Any]) -> Dict[str, Any]:
        """GET with retries and robust error handling.

        Raises
        ------
        ValueError
            On 401 unauthorized (invalid API key).
        requests.RequestException
            For retryable HTTP/network errors (e.g., 429, 5xx, timeouts).
        """
        try:
            print(f"\n{'='*50}")
            print(f"API REQUEST:")
            print(f"URL: {url}")
            print(f"Params being sent:")
            for key, value in params.items():
                print(f"  {key}: {value}")
            print(f"{'='*50}\n")
            
            resp: Response = self.session.get(url, params=params, timeout=20)
        except requests.RequestException as exc:
            logger.error("Network error while requesting %s: %s", url, exc)
            raise

        if resp.status_code == 401:
            logger.error("401 Unauthorized for %s. Check API key.", url)
            raise ValueError("Invalid API key for data.gov.in (401)")
        if resp.status_code == 404:
            logger.warning("404 Not Found for %s. Returning empty payload.", url)
            return {"records": []}
        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning("%s received for %s. Will retry.", resp.status_code, url)
            raise requests.RequestException(f"Transient HTTP error: {resp.status_code}")
        if not resp.ok:
            logger.error("HTTP error %s for %s: %s", resp.status_code, url, resp.text)
            resp.raise_for_status()

        try:
            data = resp.json()
        except ValueError as exc:  # JSON decoding error
            logger.error("Invalid JSON response from %s: %s", url, exc)
            raise requests.RequestException("Invalid JSON response")

        # Print API response details
        print(f"\n{'='*50}")
        print(f"API RESPONSE:")
        print(f"Status: {resp.status_code}")
        print(f"Records: {len(data.get('records', []))}")
        if 'records' in data and len(data['records']) > 0:
            print(f"First record: {data['records'][0]}")
        else:
            print(f"Response keys: {data.keys() if isinstance(data, dict) else 'N/A'}")
            print(f"Full response: {data}")
        print(f"{'='*50}\n")

        # Some APIs include an explicit 'status' or 'error' field
        if isinstance(data, dict) and data.get("status") == "error":
            message = data.get("message") or data.get("error") or "Unknown API error"
            logger.error("API error from %s: %s", url, message)
            raise requests.RequestException(f"API error: {message}")

        # Log successful response status
        logger.info(f"Response status: {resp.status_code}")
        
        # Log full response if no records (for debugging empty responses)
        if isinstance(data, dict):
            records = data.get("records") or data.get("data") or []
            if not records:
                logger.info(f"Full response: {data}")

        return data

    # -------------------- Utilities --------------------
    @staticmethod
    def standardize_state_name(state: str) -> str:
        """Standardize state names to title case with correct capitalization.

        Examples
        --------
        "MAHARASHTRA" -> "Maharashtra"
        "madhya pradesh" -> "Madhya Pradesh"
        "UTTAR PRADESH" -> "Uttar Pradesh"
        "andaman and nicobar islands" -> "Andaman and Nicobar Islands"
        """
        if not state:
            return state

        # Normalize whitespace and case
        words = state.strip().replace("&", "&").split()
        # Words that should remain lowercase inside names
        lowercase_words = {"and", "of", "the", "&"}

        def cap(word: str) -> str:
            w = word.lower()
            if w in lowercase_words:
                return "and" if w == "and" else w
            # Preserve acronyms like 'J&K' minimally by capitalizing first char
            return w.capitalize()

        titled = " ".join(cap(w) for w in words)
        # Special cases
        titled = titled.replace("Jammu & kashmir", "Jammu & Kashmir")
        titled = titled.replace("Andaman and nicobar islands", "Andaman and Nicobar Islands")
        titled = titled.replace("Nct of delhi", "NCT of Delhi")
        titled = titled.replace("Dadra and nagar haveli and daman and diu", "Dadra and Nagar Haveli and Daman and Diu")
        titled = titled.replace("Odisha", "Odisha")  # historically Orissa -> Odisha mapping handled elsewhere
        return titled

    @classmethod
    def map_subdivision_to_states(cls, subdivision: str) -> List[str]:
        """Return list of states under a meteorological subdivision.

        If subdivision not found, returns an empty list.
        """
        return cls.SUBDIVISION_TO_STATES.get(subdivision, [])


def _is_number(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


