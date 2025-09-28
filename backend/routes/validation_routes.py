from fastapi import APIRouter
from services.validation_service import run_validation_job

router = APIRouter()

@router.post("/")
def validate_and_sync(tables: list[str], region: str = None):
    return run_validation_job(tables, region)
