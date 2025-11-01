from backend.src.query_processor import QueryProcessor


class _FakeDataClient:
    SUBDIVISION_TO_STATES = {"Konkan & Goa": ["Maharashtra", "Goa"]}

    def __init__(self):
        self.base_url = "https://api.data.gov.in/resource/"
        self.RAINFALL_RESOURCE_ID = "rain"
        self.CROP_PRODUCTION_RESOURCE_ID = "crop"

    def standardize_state_name(self, s: str) -> str:
        return s.title()

    def fetch_rainfall_data(self, subdivision=None, year_start=None, year_end=None, limit=1000):
        return [
            {"SUBDIVISION": subdivision, "YEAR": 2019, "ANNUAL": 1000},
            {"SUBDIVISION": subdivision, "YEAR": 2020, "ANNUAL": 1100},
        ]

    def fetch_crop_production(self, state=None, district=None, crop=None, year=None, season=None, limit=1000):
        return [
            {"State_Name": state or "Maharashtra", "Crop": crop or "Wheat", "Crop_Year": year or 2020, "Area": 10, "Production": 25},
            {"State_Name": state or "Maharashtra", "Crop": crop or "Wheat", "Crop_Year": year or 2021, "Area": 12, "Production": 30},
        ]


class _FakeGemini:
    def parse_query(self, q: str):
        return {"intent": "compare_rainfall", "states": ["Maharashtra"], "years": [2019, 2020], "top_n": 5}

    def generate_response(self, user_query, data_context=None, query_results=None):
        return "ok"


def test_process_query_compare_rainfall():
    qp = QueryProcessor(_FakeDataClient(), _FakeGemini())
    out = qp.process_query("Compare rainfall in Maharashtra")
    assert out["answer"] == "ok"
    assert "sources" in out
    assert "data" in out
    assert "metadata" in out
    assert "comparisons" in out["data"]


