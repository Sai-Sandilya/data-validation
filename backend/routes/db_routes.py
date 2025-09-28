from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from mysql.connector import connect, Error

router = APIRouter()


class ConnectionRequest(BaseModel):
    host: str
    port: int = Field(default=3306, ge=1, le=65535)
    database: str
    user: str
    password: str


@router.post("/test-connection")
def test_connection(config: ConnectionRequest):
    connection = None
    try:
        connection = connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            connection_timeout=5,
        )
        if not connection.is_connected():
            raise HTTPException(status_code=400, detail="Connection failed: Unable to establish session.")
        return {"message": "Connection successful"}
    except Error as exc:
        raise HTTPException(status_code=400, detail=f"Connection failed: {exc}") from exc
    finally:
        if connection and connection.is_connected():
            connection.close()

@router.post("/get-tables")
def get_tables(config: ConnectionRequest):
    connection = None
    try:
        connection = connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            connection_timeout=5,
        )
        if not connection.is_connected():
            raise HTTPException(status_code=400, detail="Connection failed: Unable to establish session.")
        
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        return {"tables": tables, "database": config.database}
    except Error as exc:
        raise HTTPException(status_code=400, detail=f"Failed to fetch tables: {exc}") from exc
    finally:
        if connection and connection.is_connected():
            connection.close()
