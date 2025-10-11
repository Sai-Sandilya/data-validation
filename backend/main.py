from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from routes import db_routes, comparison_routes, lazy_evaluation_routes, pyspark_routes
# Temporarily disabled: production_routes (causing startup issues)

app = FastAPI(title="Data Validation & Sync System")

# Add CORS middleware (allow all during development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=600,
)

# Explicit OPTIONS handler to satisfy strict preflight checks
@app.options("/{rest_of_path:path}")
def preflight_handler(rest_of_path: str):
    return JSONResponse({"ok": True})

# PySpark + Lazy Evaluation Routes
app.include_router(db_routes.router, prefix="/db", tags=["Database"])
app.include_router(comparison_routes.router, prefix="/compare", tags=["Comparison"])
app.include_router(lazy_evaluation_routes.router, prefix="/lazy-eval", tags=["Lazy Evaluation"])
app.include_router(pyspark_routes.router, prefix="/pyspark", tags=["PySpark + Lazy Evaluation"])
# app.include_router(production_routes.router, prefix="/production", tags=["Production Features"])  # Temporarily disabled

@app.get("/")
def root():
    return {"message": "Welcome to Data Validation & Sync API"}

@app.post("/test-endpoint")
def test_endpoint():
    return {"message": "Test endpoint working", "status": "success"}

@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "Backend is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
