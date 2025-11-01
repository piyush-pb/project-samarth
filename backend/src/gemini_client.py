import json
import logging
from typing import Any, Dict, List, Mapping, Optional, Tuple

import google.generativeai as genai
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GeminiClient:
    """Wrapper around Google Gemini for query parsing and response generation.

    Parameters
    ----------
    api_key: str
        Google AI Studio (Gemini) API key.
    model_name: str, optional
        Model identifier. Defaults to "gemini-1.5-pro".
    temperature: float, optional
        Decoding temperature. Defaults to 0.1 for accuracy.
    max_tokens: int, optional
        Maximum output tokens. Defaults to 2000.
    """

    def __init__(
        self,
        api_key: str,
        *,
        model_name: str = "gemini-2.5-pro",
        temperature: float = 0.1,
        max_tokens: int = 2000,
    ) -> None:
        if not api_key:
            raise ValueError("Gemini API key is required")

        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model_name,
            generation_config={
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            },
        )

    def get_parse_prompt(self, user_query: str) -> str:
        """Generate the parsing prompt for Gemini"""
        
        return f"""You are a query parser for Indian agricultural and climate data.

Extract structured information from this query. Be thorough and catch ALL mentioned entities.

Query: {user_query}

Return ONLY valid JSON (no markdown, no code blocks, just the JSON object):

{{
  "intent": "compare_rainfall" | "compare_crops" | "correlation" | "identify_district" | "analyze_trend" | "policy_analysis" | "general",
  "states": ["State1", "State2"],
  "districts": ["District1"],
  "crops": ["Crop1"],
  "crop_types": ["Cereal", "Pulse"],
  "years": [start_year, end_year],
  "seasons": [],
  "metrics": ["rainfall", "production"],
  "comparison_type": "between_states",
  "top_n": 5
}}

IMPORTANT RULES:
1. If query mentions "last N years", calculate from 2005 (latest crop data): years = [2005-N+1, 2005]
2. If query mentions "last 5 years": years = [2001, 2005]
3. Extract ALL state names mentioned in the query
4. If query asks for both rainfall AND crops, intent should be "correlation"
5. If query asks for "cereals", add crop_types: ["Cereal"]
6. Cereal crops include: Rice, Wheat, Maize, Bajra, Jowar, Ragi, Barley
7. Always extract the top_n number if mentioned (e.g., "top 5" means top_n: 5)

Examples:

Query: "Compare rainfall in Maharashtra and Gujarat for last 5 years"
{{"intent": "compare_rainfall", "states": ["Maharashtra", "Gujarat"], "districts": [], "crops": [], "crop_types": [], "years": [2011, 2015], "seasons": [], "metrics": ["rainfall"], "comparison_type": "between_states", "top_n": 5}}

Query: "Compare average annual rainfall in Maharashtra and Gujarat for last 5 years. List top 5 cereals by production in each state during same period"
{{"intent": "correlation", "states": ["Maharashtra", "Gujarat"], "districts": [], "crops": [], "crop_types": ["Cereal"], "years": [2001, 2005], "seasons": [], "metrics": ["rainfall", "production"], "comparison_type": "between_states", "top_n": 5}}

Query: "Top 5 wheat producing districts in Punjab in 2003"
{{"intent": "compare_crops", "states": ["Punjab"], "districts": [], "crops": ["Wheat"], "crop_types": [], "years": [2003, 2003], "seasons": [], "metrics": ["production"], "comparison_type": "between_districts", "top_n": 5}}

Now parse this query and return ONLY the JSON object:
Query: {user_query}"""

    def parse_query(self, user_query: str) -> Dict[str, Any]:
        """Parse a natural language query into a structured JSON schema.

        Uses an exact prompt to enforce a strict JSON-only response.

        Parameters
        ----------
        user_query: str
            The user's natural language question.

        Returns
        -------
        Dict[str, Any]
            Parsed JSON containing intent, states, districts, crops, etc.
        """
        if not user_query:
            raise ValueError("user_query is required")

        response_text = ""
        try:
            prompt = self.get_parse_prompt(user_query)
            response = self.model.generate_content(prompt)
            
            # Get the text response
            if response and response.text:
                response_text = response.text.strip()
            else:
                response_text = ""
            
            print(f"\n{'='*50}")
            print(f"GEMINI RAW RESPONSE:")
            print(response_text)
            print(f"{'='*50}\n")
            
            # Remove markdown code blocks if present
            if '```json' in response_text:
                start = response_text.find('```json') + 7
                end = response_text.find('```', start)
                if end != -1:
                    response_text = response_text[start:end].strip()
            elif '```' in response_text:
                start = response_text.find('```') + 3
                end = response_text.find('```', start)
                if end != -1:
                    response_text = response_text[start:end].strip()
            
            # Parse JSON
            parsed = json.loads(response_text)
            
            print(f"\n{'='*50}")
            print(f"PARSED RESULT:")
            print(f"Intent: {parsed.get('intent')}")
            print(f"States: {parsed.get('states')}")
            print(f"Years: {parsed.get('years')}")
            print(f"Metrics: {parsed.get('metrics')}")
            print(f"{'='*50}\n")
            
            # Validate and set defaults
            if 'intent' not in parsed or not parsed['intent']:
                parsed['intent'] = 'general'
            if 'states' not in parsed or not parsed['states']:
                parsed['states'] = []
            if 'years' not in parsed or not parsed['years']:
                parsed['years'] = [2001, 2005]
            if 'top_n' not in parsed:
                parsed['top_n'] = 5
            if 'metrics' not in parsed:
                parsed['metrics'] = ['production']
            
            # Validate and normalize using existing helper
            return _normalize_parsed_query(parsed)
            
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Error parsing query with Gemini: {exc}")
            if 'response_text' in locals():
                logger.error(f"Response text: {response_text}")
            
            # Return a safe default with the original query
            return {
                'intent': 'general',
                'states': [],
                'districts': [],
                'crops': [],
                'crop_types': [],
                'years': [2001, 2005],
                'seasons': [],
                'metrics': ['production'],
                'comparison_type': 'none',
                'top_n': 5,
                'original_query': user_query
            }

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),  # Network/timeouts/rate limits - retry up to 3
        wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
        retry=retry_if_exception_type((RuntimeError,)),
    )
    def generate_response(
        self,
        user_query: str,
        data_context: str,
        query_results: Dict[str, Any],
    ) -> str:
        """Generate natural language answer with citations"""

        prompt = f"""You are an expert agricultural data analyst for India.



User Question: {user_query}



Retrieved Data Context:

{data_context}



Query Results Summary:

{json.dumps(query_results, indent=2)}



Instructions:

1. Answer the user's question directly using the available data

2. If the data is from a different time period than requested (e.g., user asks for "last 5 years" but data is from 2001-2005), explain this naturally at the start and proceed with the analysis

3. For EVERY specific data point, number, or claim, cite the source using this format: [Source: Dataset Name, Filters: Key=Value, Records: N]

4. Structure your response:

   - Brief note if time period differs from request

   - Direct answer with comparisons

   - Detailed breakdown with numbers

   - Key insights

5. Be precise with numbers, use proper formatting (e.g., "2,913,600 tonnes")

6. Present the data professionally

7. Do NOT make up any data



Example citation format:

"Maharashtra's top cereal crop was Jowar with 2,913,600 tonnes of production during 2001-2005 [Source: District-wise Crop Production, Filters: State=Maharashtra Years=2001-2005 Crop_Types=Cereal, Records: 166]"



"The Gujarat Region subdivision received an average annual rainfall of 850mm during 2001-2005 [Source: Area-weighted Rainfall Data, Filters: Subdivision=Gujarat Region Years=2001-2005, Records: 5]"



Generate a comprehensive, professional response:

"""

        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            return "An error occurred while generating the response."

def _normalize_parsed_query(obj: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate and coerce parsed query to required schema with sane defaults."""
    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    out: Dict[str, Any] = {
        "intent": str(obj.get("intent") or "general"),
        "states": [str(x) for x in _as_list(obj.get("states"))],
        "districts": [str(x) for x in _as_list(obj.get("districts"))],
        "crops": [str(x) for x in _as_list(obj.get("crops"))],
        "crop_types": [str(x) for x in _as_list(obj.get("crop_types"))],
        "years": _normalize_years(obj.get("years")),
        "seasons": [str(x) for x in _as_list(obj.get("seasons"))],
        "metrics": [str(x) for x in _as_list(obj.get("metrics"))],
        "comparison_type": str(obj.get("comparison_type") or "none"),
        "top_n": int(obj.get("top_n") or 5),
    }
    return out


def _normalize_years(years: Any) -> List[int]:
    if isinstance(years, list) and len(years) == 2:
        try:
            return [int(years[0]), int(years[1])]
        except Exception:  # noqa: BLE001
            return []
    return []


def _safe_json_dumps(data: Mapping[str, Any]) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        return str(data)


def _summarize_results_for_prompt(rows: List[Mapping[str, Any]], max_items: int = 8) -> str:
    summary_items: List[Mapping[str, Any]] = []
    for i, r in enumerate(rows):
        if i >= max_items:
            break
        # Keep only a small subset of fields if present
        picked: Dict[str, Any] = {}
        for key in ("state", "district", "crop", "season", "year", "ANNUAL", "avg_rainfall", "Area", "Production", "records", "filters", "source"):
            if key in r:
                picked[key] = r[key]
        if not picked:
            picked = dict(list(r.items())[:6])
        summary_items.append(picked)
    return json.dumps(summary_items, ensure_ascii=False, default=str)


