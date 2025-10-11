"""
Safe cleanup script - Deletes unnecessary files without impacting the product
Run this to remove test scripts, unused components, and duplicate files
"""
import os
import shutil
from pathlib import Path

# List of files to delete (verified safe)
FILES_TO_DELETE = [
    # Backend test scripts
    "backend/test_api_endpoints.py",
    "backend/test_flexible_scd_system.py",
    "backend/test_mysql_only.py",
    "backend/test_mysql_spark.py",
    "backend/test_phase1_schema_discovery.py",
    "backend/test_phase2_frontend_integration.py",
    "backend/test_phase3_intelligent_sync.py",
    "backend/test_phase4_production_features.py",
    "backend/test_scd_detection.py",
    "backend/test_simple_validation.py",
    "backend/test_ultra_simple_interface.py",
    "backend/mysql_test.py",
    "backend/mysql_default_test.py",
    "backend/simple_test.py",
    
    # Backend check/verify scripts
    "backend/check_alex_history.py",
    "backend/check_databases.py",
    "backend/check_sync_logs.py",
    "backend/check_sync_results.py",
    "backend/show_current_scd2_records.py",
    "backend/show_scd_samples.py",
    "backend/verify_all_careers.py",
    "backend/verify_comprehensive_data.py",
    "backend/verify_scd2_sync.py",
    
    # Backend setup/seed scripts (one-time use)
    "backend/add_comprehensive_scd2_data.py",
    "backend/add_more_scd_test_data.py",
    "backend/add_more_test_data.py",
    "backend/create_multi_table_scd_system.py",
    "backend/create_rich_scd2_data.py",
    "backend/setup_comprehensive_test_data.py",
    "backend/setup_enhanced_test_data.py",
    "backend/setup_sample_data.py",
    "backend/setup_scd_test_data.py",
    "backend/setup_scd2_table.py",
    
    # Duplicate cleanup scripts
    "backend/cleanup_all_tables.py",
    
    # Guide files
    "backend/spark_setup_guide.py",
    
    # SQL scripts (already executed)
    "backend/cleanup_scd2_tables.sql",
    
    # Frontend unused components
    "frontend/src/components/ConfigForm.tsx",
    "frontend/src/components/EnhancedTableSelector.tsx",
    "frontend/src/components/ReportTable.tsx",
    "frontend/src/components/SimpleReportTable.tsx",
    "frontend/src/components/TableSelector.tsx",
    
    # Frontend unused pages
    "frontend/src/pages/ConfigPage.tsx",
    "frontend/src/pages/ReportPage.tsx",
    "frontend/src/pages/SQLQueryPage.tsx",
]

# Directories to delete __pycache__ from
PYCACHE_DIRS = [
    "backend/__pycache__",
    "backend/config/__pycache__",
    "backend/core/__pycache__",
    "backend/routes/__pycache__",
    "backend/services/__pycache__",
    "backend/venv/Scripts/__pycache__",
    "spark_jobs/__pycache__",
]

def delete_file(filepath):
    """Delete a single file"""
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"  [OK] Deleted: {filepath}")
            return True
        else:
            print(f"  [SKIP] Not found: {filepath}")
            return False
    except Exception as e:
        print(f"  [ERROR] Error deleting {filepath}: {e}")
        return False

def delete_directory(dirpath):
    """Delete a directory and all contents"""
    try:
        if os.path.exists(dirpath):
            shutil.rmtree(dirpath)
            print(f"  [OK] Deleted directory: {dirpath}")
            return True
        else:
            print(f"  [SKIP] Not found: {dirpath}")
            return False
    except Exception as e:
        print(f"  [ERROR] Error deleting {dirpath}: {e}")
        return False

def main():
    print("=" * 80)
    print("SAFE CLEANUP SCRIPT - Data Validation & Sync System")
    print("=" * 80)
    print("\nThis will delete:")
    print("  - Test scripts (28 files)")
    print("  - Setup/seed scripts (10 files)")
    print("  - Unused frontend components (5 files)")
    print("  - Unused frontend pages (3 files)")
    print("  - Duplicate utilities (2 files)")
    print("  - __pycache__ directories")
    print("\n[WARNING] This action cannot be undone!")
    print("=" * 80)
    
    # Confirm
    confirm = input("\nType 'DELETE' to proceed: ").strip()
    
    if confirm != "DELETE":
        print("\n[CANCELLED] Cleanup cancelled.")
        return
    
    print("\n" + "=" * 80)
    print("STARTING CLEANUP...")
    print("=" * 80)
    
    deleted_files = 0
    skipped_files = 0
    errors = 0
    
    # Delete files
    print("\n[FILES] Deleting files...")
    for filepath in FILES_TO_DELETE:
        result = delete_file(filepath)
        if result:
            deleted_files += 1
        elif os.path.exists(filepath):
            errors += 1
        else:
            skipped_files += 1
    
    # Delete __pycache__ directories
    print("\n[DIRECTORIES] Deleting __pycache__ directories...")
    for dirpath in PYCACHE_DIRS:
        if delete_directory(dirpath):
            deleted_files += 1
        else:
            skipped_files += 1
    
    # Summary
    print("\n" + "=" * 80)
    print("CLEANUP SUMMARY")
    print("=" * 80)
    print(f"[SUCCESS] Files/Directories Deleted: {deleted_files}")
    print(f"[SKIPPED] Not found: {skipped_files}")
    print(f"[ERRORS] Errors: {errors}")
    
    if errors == 0:
        print("\n[COMPLETE] Cleanup completed successfully!")
        print("\n[KEPT] What was kept:")
        print("  - All core backend files (main.py, routes, services, core)")
        print("  - All active frontend files (App.tsx, ValidationPage, UltraSimpleReportTable, SQLQuery)")
        print("  - Configuration files (settings, requirements.txt, package.json)")
        print("  - Documentation (README.md, FLEXIBLE_SCD_SYSTEM.md)")
        print("  - One cleanup utility (auto_cleanup_tables.py)")
        print("  - Database setup script (setup_databases.sql)")
    else:
        print(f"\n[WARNING] Cleanup completed with {errors} error(s).")
    
    print("=" * 80)

if __name__ == "__main__":
    main()

