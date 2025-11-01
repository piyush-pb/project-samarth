# Deployment Guide

## Frontend (Netlify)

The frontend is configured for Netlify deployment.

### Quick Deploy

1. **Push code to GitHub** (already done)

2. **Connect to Netlify:**
   - Go to [Netlify](https://www.netlify.com)
   - Click "New site from Git"
   - Select GitHub and authorize
   - Choose `project-samarth` repository
   - Netlify will auto-detect settings from `netlify.toml`

3. **Set Environment Variables:**
   - Go to Site settings → Environment variables
   - Add: `VITE_API_URL` = `https://your-backend-url.com` (see backend deployment)

4. **Deploy:**
   - Netlify will automatically build and deploy
   - The build command runs: `cd frontend && npm install && npm run build`

## Backend (Recommended: Render or Railway)

Since Netlify doesn't support Python servers, deploy the backend separately.

### Option 1: Render

1. **Create new Web Service:**
   - Go to [Render](https://render.com)
   - Click "New" → "Web Service"
   - Connect your GitHub repo

2. **Configure:**
   - **Build Command:** `cd backend && pip install -r requirements.txt`
   - **Start Command:** `cd backend && python -m uvicorn src.main:app --host 0.0.0.0 --port $PORT`
   - **Environment:** Python 3

3. **Set Environment Variables:**
   - `GOOGLE_API_KEY` - Your Google Gemini API key
   - `DATA_GOV_API_KEY` - Your data.gov.in API key
   - `DATA_GOV_BASE_URL` - `https://api.data.gov.in/resource/` (default)

4. **Update CORS:**
   - Update `backend/src/main.py` CORS origins to include your Netlify URL

### Option 2: Railway

1. **Create new project:**
   - Go to [Railway](https://railway.app)
   - Click "New Project" → "Deploy from GitHub"
   - Select your repo

2. **Configure:**
   - Set root directory to `backend`
   - Railway auto-detects Python and requirements.txt
   - Add environment variables same as Render

3. **Update CORS:**
   - Update `backend/src/main.py` CORS origins to include your Netlify URL

## Update Frontend API URL

Once backend is deployed:

1. Copy your backend URL (e.g., `https://project-samarth.onrender.com`)
2. In Netlify dashboard:
   - Go to Site settings → Environment variables
   - Set `VITE_API_URL` = `https://project-samarth.onrender.com`

## Local Development

### Frontend:
```bash
cd frontend
npm install
npm run dev
```

### Backend:
```bash
cd backend
pip install -r requirements.txt
python -m uvicorn src.main:app --reload
```

Frontend will proxy `/api/*` to `http://localhost:8000` automatically.

