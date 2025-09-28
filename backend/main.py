from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from routes import db_routes, validation_routes, report_routes, comparison_routes, sql_routes, audit_routes

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

app.include_router(db_routes.router, prefix="/db", tags=["Database"])
app.include_router(validation_routes.router, prefix="/validate", tags=["Validation"])
app.include_router(report_routes.router, prefix="/report", tags=["Reports"])
app.include_router(comparison_routes.router, prefix="/compare", tags=["Comparison"])
app.include_router(sql_routes.router, prefix="/sql", tags=["SQL Query"])
app.include_router(audit_routes.router, prefix="/audit", tags=["Audit & Logging"])

@app.get("/")
def root():
    return {"message": "Welcome to Data Validation & Sync API"}

@app.post("/test-endpoint")
def test_endpoint():
    return {"message": "Test endpoint working", "status": "success"}
