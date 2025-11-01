# Project Samarth

Intelligent Q&A System for Indian Agricultural & Climate Data

## Overview
This system provides a natural language interface to query Indian agricultural and climate data from data.gov.in using Google Gemini for query understanding and response generation.

## Features
- Natural language queries about agriculture and climate
- Real-time data fetching from data.gov.in APIs
- Intelligent query parsing using Google Gemini
- Source citations for all data points
- Professional chat interface (React + TailwindCSS)
- Supports complex queries across multiple datasets

## Architecture
- **Backend**: FastAPI + Python
- **LLM**: Google Gemini 1.5 Pro
- **Data Source**: data.gov.in API
- **Frontend**: React + Vite + TailwindCSS

## Datasets Used
1. District-wise Crop Production (1997+)
   - Resource ID: 9ef84268-d588-465a-a308-a864a43d0070
   - Columns: State, District, Year, Season, Crop, Area, Production
2. Area-weighted Rainfall Data
   - Resource ID: d1bf81a2-7142-4cf6-ac71-83e1f16cf54f
   - Columns: SUBDIVISION, YEAR, JAN..DEC, ANNUAL

## Backend Setup
Requirements: Python 3.10+

1) Install dependencies
```bash
pip install -r backend/requirements.txt
```

2) Create environment file `backend/.env`
```bash
# Google AI Studio (Gemini) API key
GOOGLE_API_KEY=your_google_ai_studio_key_here

# data.gov.in API key
DATA_GOV_API_KEY=your_data_gov_api_key_here

# Data.gov.in resource IDs (optional override)
CROP_PRODUCTION_RESOURCE_ID=9ef84268-d588-465a-a308-a864a43d0070
RAINFALL_RESOURCE_ID=d1bf81a2-7142-4cf6-ac71-83e1f16cf54f

# Base URL
DATA_GOV_BASE_URL=https://api.data.gov.in/resource/
```

3) Run the API server
```bash
uvicorn backend.src.main:app --host 0.0.0.0 --port 8000 --reload
```

## Frontend Setup
Requirements: Node.js 18+

```bash
cd frontend
npm install
npm run dev
```

Dev server runs on http://localhost:5173 and proxies `/api` to `http://localhost:8000`.

## API Endpoints
- `POST /api/query` → Process natural language query
- `GET /api/health` → Health status of components
- `GET /api/sample-questions` → Curated sample questions
- `POST /api/test-data-api` → Diagnostic sample fetch (debug)

### Query Request
```json
{ "query": "Compare rainfall in Maharashtra and Gujarat for last 5 years" }
```

### Query Response (shape)
```json
{
  "answer": "...",
  "sources": [
    {
      "dataset": "District-wise Crop Production",
      "url": "https://www.data.gov.in/resource/...",
      "resource_id": "9ef84268-...",
      "filters_applied": { "State_Name": "Maharashtra", "Crop_Year": 2023 },
      "records_retrieved": 150
    }
  ],
  "data": {
    "states": {},
    "comparisons": {},
    "statistics": {}
  },
  "metadata": {
    "query_time": "2024-10-31T10:30:00",
    "processing_time_seconds": 3.5,
    "data_sources_queried": 2,
    "total_records_processed": 500
  }
}
```

## Development
Key backend modules (under `backend/src/`):
- `data_api_client.py` — data.gov.in client (pagination, caching, retries)
- `gemini_client.py` — Google Gemini integration (parse + generate)
- `query_processor.py` — Orchestration logic for intents and aggregation
- `main.py` — FastAPI app and endpoints

## Running Tests
```bash
pytest -q
```

## Security Notes
- Keep API keys in `backend/.env` and never commit them.
- The server sanitizes inputs and handles API errors with retries and backoff.

## License
For internal evaluation and demo purposes.


