from fastapi import APIRouter
from services.sync_service import perform_sync

router = APIRouter()

@router.get("/")
def get_report():
    # TODO: Replace with real DB report
    return [
        {
            "source_table": "customers",
            "target_table": "customers",
            "matched": 95,
            "unmatched": 5,
            "sync_status": "Pending"
        }
    ]

@router.post("/sync")
def sync_table(table: str):
    result = perform_sync(table)
    return {"status": "success", "table": table, "message": result}
