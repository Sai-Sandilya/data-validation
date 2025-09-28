from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import mysql.connector
from mysql.connector import Error
from config.db_config import SOURCE_DB, TARGET_DB

router = APIRouter()

class SQLQueryRequest(BaseModel):
    query: str
    database: str  # 'source' or 'target'
    limit: Optional[int] = 1000  # Safety limit

class SQLQueryResult(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    row_count: int
    message: str
    execution_time_ms: float

@router.post("/execute-query", response_model=SQLQueryResult)
def execute_sql_query(request: SQLQueryRequest):
    """Execute SQL query on source or target database"""
    
    # Security: Only allow safe read-only statements
    query_upper = request.query.strip().upper()
    allowed_commands = ['SELECT', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN']
    
    if not any(query_upper.startswith(cmd) for cmd in allowed_commands):
        raise HTTPException(
            status_code=400, 
            detail="Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed for security reasons"
        )
    
    # Choose database configuration
    db_config = SOURCE_DB if request.database == 'source' else TARGET_DB
    
    connection = None
    start_time = None
    
    try:
        import time
        start_time = time.time()
        
        connection = mysql.connector.connect(
            host=db_config["host"],
            port=int(db_config["port"]),
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            connection_timeout=10,
        )
        
        cursor = connection.cursor(dictionary=True)
        
        # Add LIMIT if not present and limit is specified
        query = request.query.strip()
        if request.limit and 'LIMIT' not in query_upper:
            query += f" LIMIT {request.limit}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        cursor.close()
        
        return SQLQueryResult(
            success=True,
            data=results,
            columns=columns,
            row_count=len(results),
            message=f"Query executed successfully. Retrieved {len(results)} rows.",
            execution_time_ms=round(execution_time, 2)
        )
        
    except Error as e:
        execution_time = (time.time() - start_time) * 1000 if start_time else 0
        return SQLQueryResult(
            success=False,
            data=None,
            columns=None,
            row_count=0,
            message=f"Database error: {str(e)}",
            execution_time_ms=round(execution_time, 2)
        )
    except Exception as e:
        execution_time = (time.time() - start_time) * 1000 if start_time else 0
        return SQLQueryResult(
            success=False,
            data=None,
            columns=None,
            row_count=0,
            message=f"Query execution error: {str(e)}",
            execution_time_ms=round(execution_time, 2)
        )
    finally:
        if connection and connection.is_connected():
            connection.close()

@router.get("/sample-queries")
def get_sample_queries():
    """Get sample SQL queries for common operations"""
    return {
        "sample_queries": [
            {
                "name": "Check all customers in target",
                "query": "SELECT * FROM customers",
                "description": "View all customer records in the database"
            },
            {
                "name": "Check updated records",
                "query": "SELECT * FROM customers WHERE record_status = 'UPDATED'",
                "description": "View only records that were synced/updated"
            },
            {
                "name": "Count records by status",
                "query": "SELECT record_status, COUNT(*) as count FROM customers GROUP BY record_status",
                "description": "Count records grouped by their status"
            },
            {
                "name": "Check specific customer",
                "query": "SELECT * FROM customers WHERE id = 1",
                "description": "View details of a specific customer by ID"
            },
            {
                "name": "Compare source vs target counts",
                "query": "SELECT COUNT(*) as total_records FROM customers",
                "description": "Get total record count (run on both databases to compare)"
            },
            {
                "name": "Recent updates",
                "query": "SELECT * FROM customers WHERE record_status IN ('UPDATED', 'ACTIVE') ORDER BY id",
                "description": "View all active and updated records"
            },
            {
                "name": "Check table structure",
                "query": "DESCRIBE customers",
                "description": "View the table structure and column definitions"
            },
            {
                "name": "Show all tables",
                "query": "SHOW TABLES",
                "description": "List all tables in the database"
            }
        ]
    }

@router.get("/database-info/{database}")
def get_database_info(database: str):
    """Get basic information about source or target database"""
    
    if database not in ['source', 'target']:
        raise HTTPException(status_code=400, detail="Database must be 'source' or 'target'")
    
    db_config = SOURCE_DB if database == 'source' else TARGET_DB
    
    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_config["host"],
            port=int(db_config["port"]),
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            connection_timeout=10,
        )
        
        cursor = connection.cursor()
        
        # Get database name
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]
        
        # Get tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Get table info for each table
        table_info = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            table_info[table] = {"row_count": count}
        
        cursor.close()
        
        return {
            "database_name": db_name,
            "host": db_config["host"],
            "port": db_config["port"],
            "tables": table_info,
            "connection_status": "Connected"
        }
        
    except Error as e:
        return {
            "database_name": db_config.get("database", "Unknown"),
            "host": db_config["host"],
            "port": db_config["port"],
            "tables": {},
            "connection_status": f"Error: {str(e)}"
        }
    finally:
        if connection and connection.is_connected():
            connection.close()
