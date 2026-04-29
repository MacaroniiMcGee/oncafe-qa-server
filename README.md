# OnCAFE QA Tool Server

## Deploy to Railway (free)

1. Go to railway.app → New Project → Deploy from GitHub
2. Push this folder to a GitHub repo
3. Set environment variable: AUTH_KEY=your-secret-key
4. Railway gives you a URL like: https://oncafe-qa.up.railway.app

## Local dev
npm install
npm start

## Endpoints
GET  /           → server info
GET  /tool       → latest HTML tool
POST /tool       → save updated HTML (header: x-auth-key)
GET  /versions   → list all saved versions
GET  /version/42 → restore version 42
