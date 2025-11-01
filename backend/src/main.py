import logging
import os
import time
from datetime import datetime
from typing import Dict, List

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .data_api_client import DataGovAPIClient
from .gemini_client import GeminiClient
from .query_processor import QueryProcessor

# -----------------------------------------------------------------------------
# App initialization
# -----------------------------------------------------------------------------
app = FastAPI(
    title="Project Samarth API",
    description="Intelligent Q&A System for Indian Agricultural & Climate Data",
    version="1.0.0",
)


# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logger = logging.getLogger("project_samarth")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
class QueryRequest(BaseModel):
    query: str


class QueryResponse(BaseModel):
    answer: str
    sources: List[Dict]
    data: Dict
    metadata: Dict


# -----------------------------------------------------------------------------
# Request logging middleware
# -----------------------------------------------------------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    logger.info("Incoming %s %s", request.method, request.url.path)
    try:
        response = await call_next(request)
        duration = round(time.time() - start, 3)
        logger.info("Completed %s %s in %ss with status %s", request.method, request.url.path, duration, response.status_code)
        return response
    except Exception as exc:  # noqa: BLE001
        duration = round(time.time() - start, 3)
        logger.exception("Error processing %s %s in %ss: %s", request.method, request.url.path, duration, exc)
        raise


# -----------------------------------------------------------------------------
# Startup: load env and initialize clients
# -----------------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    load_dotenv()
    logger.info("Starting Project Samarth API...")

    google_api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    data_gov_api_key = os.getenv("DATA_GOV_API_KEY", "").strip()
    base_url = os.getenv("DATA_GOV_BASE_URL", "https://api.data.gov.in/resource/").strip()

    if not data_gov_api_key:
        logger.warning("DATA_GOV_API_KEY is not set; data API calls will fail.")
    if not google_api_key:
        logger.warning("GOOGLE_API_KEY is not set; Gemini calls will fail.")

    data_client = DataGovAPIClient(api_key=data_gov_api_key, base_url=base_url)

    # Allow overriding resource IDs via env if provided
    crop_res_id = os.getenv("CROP_PRODUCTION_RESOURCE_ID")
    rain_res_id = os.getenv("RAINFALL_RESOURCE_ID")
    if crop_res_id:
        data_client.CROP_PRODUCTION_RESOURCE_ID = crop_res_id
    if rain_res_id:
        data_client.RAINFALL_RESOURCE_ID = rain_res_id

    gemini_client = GeminiClient(api_key=google_api_key)
    processor = QueryProcessor(data_client=data_client, gemini_client=gemini_client)

    app.state.data_client = data_client
    app.state.gemini_client = gemini_client
    app.state.processor = processor

    logger.info("Startup complete: clients initialized.")


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.post("/api/query", response_model=QueryResponse)
async def process_query(req: QueryRequest):
    if not req.query or not req.query.strip():
        raise HTTPException(status_code=400, detail={
            "error": "Invalid Request",
            "message": "Query must not be empty.",
            "timestamp": datetime.utcnow().isoformat(),
        })

    if not hasattr(app.state, "processor"):
        raise HTTPException(status_code=500, detail={
            "error": "Server Error",
            "message": "Server not initialized properly.",
            "timestamp": datetime.utcnow().isoformat(),
        })

    start = time.time()
    try:
        result = app.state.processor.process_query(req.query)
        # Ensure metadata exists and add total processing time at API layer
        result.setdefault("metadata", {})
        result["metadata"]["api_processing_time_seconds"] = round(time.time() - start, 3)
        return result
    except ValueError as ve:
        logger.warning("Bad request: %s", ve)
        raise HTTPException(status_code=400, detail={
            "error": "Bad Request",
            "message": str(ve),
            "timestamp": datetime.utcnow().isoformat(),
        })
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled error in /api/query: %s", exc)
        error_msg = str(exc) or "An unexpected error occurred while processing the query."
        raise HTTPException(status_code=500, detail={
            "error": "Server Error",
            "message": error_msg,
            "timestamp": datetime.utcnow().isoformat(),
        })


@app.get("/api/health")
async def health():
    ts = datetime.utcnow().isoformat()
    components = {
        "data_api": "unknown",
        "gemini_api": "unknown",
        "query_processor": "unknown",
    }

    try:
        processor_ok = hasattr(app.state, "processor") and app.state.processor is not None
        components["query_processor"] = "ready" if processor_ok else "not_ready"
    except Exception:
        components["query_processor"] = "not_ready"

    # Light data.gov.in check
    try:
        dc: DataGovAPIClient = app.state.data_client
        _ = dc.fetch_crop_production(limit=1)
        components["data_api"] = "connected"
    except Exception as exc:  # noqa: BLE001
        logger.warning("Data API health check failed: %s", exc)
        components["data_api"] = "error"

    # Light Gemini check
    try:
        gc: GeminiClient = app.state.gemini_client
        _ = gc.parse_query("health check")
        components["gemini_api"] = "connected"
    except Exception as exc:  # noqa: BLE001
        logger.warning("Gemini health check failed: %s", exc)
        components["gemini_api"] = "error"

    status = "healthy" if all(v in ("connected", "ready") for v in components.values()) else "degraded"
    return {"status": status, "timestamp": ts, "components": components}


@app.get("/api/sample-questions")
async def sample_questions():
    return [
        "Compare the average annual rainfall in Maharashtra and Gujarat for the last 5 years. In parallel, list the top 5 most produced cereals by volume in each state during the same period.",
        "Identify the district in Punjab with the highest wheat production in 2023 and compare that with the district with the lowest wheat production in Haryana.",
        "Analyze the rice production trend in West Bengal over the last decade. Correlate this trend with the corresponding rainfall data for the same period.",
        "A policy advisor is proposing a scheme to promote millets over rice in Karnataka. Based on historical data from the last 10 years, what are the three most compelling data-backed arguments to support this policy?",
    ]


@app.post("/api/test-data-api")
async def test_data_api():
    try:
        dc: DataGovAPIClient = app.state.data_client
        crop = dc.fetch_crop_production(limit=5)
        rain = dc.fetch_rainfall_data(subdivision="Konkan & Goa", year_start=2019, year_end=2023, limit=5)
        return {"crop_sample": crop[:5], "rain_sample": rain[:5]}
    except Exception as exc:  # noqa: BLE001
        logger.exception("/api/test-data-api failed: %s", exc)
        raise HTTPException(status_code=500, detail={
            "error": "Server Error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
        })


# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


