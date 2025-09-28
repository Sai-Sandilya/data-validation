import subprocess
import json

def run_validation_job(tables, region):
    """
    Trigger PySpark job externally to avoid blocking FastAPI thread.
    """
    cmd = [
        "spark-submit", "spark_jobs/validator.py",
        "--tables", ",".join(tables),
        "--region", region or ""
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        return {"error": stderr.decode("utf-8")}
    
    try:
        return json.loads(stdout.decode("utf-8"))
    except:
        return {"raw_output": stdout.decode("utf-8")}
