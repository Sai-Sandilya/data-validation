# 🗑️ FILE CLEANUP ANALYSIS

## ✅ CORE FILES (MUST KEEP)

### Backend Core
- `backend/main.py` - Main FastAPI application
- `backend/requirements.txt` - Python dependencies
- `backend/config/` - Configuration files
- `backend/core/` - Core utilities (adapters, sync engine, utils)
- `backend/services/` - Business logic services
- `backend/routes/` - API endpoints (KEEP ALL except production_routes.py if not used)

### Frontend Core
- `frontend/src/App.tsx` - Main React component
- `frontend/src/main.tsx` - Entry point
- `frontend/src/pages/ValidationPage.tsx` - Main page (ACTIVE)
- `frontend/src/components/UltraSimpleReportTable.tsx` - Main table component (ACTIVE)
- `frontend/src/components/SQLQuery.tsx` - SQL interface (ACTIVE)
- `frontend/package.json`, `vite.config.ts`, etc. - Build config

### Documentation
- `README.md` - Project documentation
- `FLEXIBLE_SCD_SYSTEM.md` - SCD documentation
- `INTERFACE_SWITCHED.md` - Interface docs

---

## ❌ SAFE TO DELETE (33 FILES)

### Test Scripts (28 files) - No longer needed after development
1. `backend/test_api_endpoints.py`
2. `backend/test_flexible_scd_system.py`
3. `backend/test_mysql_only.py`
4. `backend/test_mysql_spark.py`
5. `backend/test_phase1_schema_discovery.py`
6. `backend/test_phase2_frontend_integration.py`
7. `backend/test_phase3_intelligent_sync.py`
8. `backend/test_phase4_production_features.py`
9. `backend/test_scd_detection.py`
10. `backend/test_simple_validation.py`
11. `backend/test_ultra_simple_interface.py`
12. `backend/mysql_test.py`
13. `backend/mysql_default_test.py`
14. `backend/simple_test.py`
15. `backend/check_alex_history.py`
16. `backend/check_databases.py`
17. `backend/check_sync_logs.py`
18. `backend/check_sync_results.py`
19. `backend/show_current_scd2_records.py`
20. `backend/show_scd_samples.py`
21. `backend/verify_all_careers.py`
22. `backend/verify_comprehensive_data.py`
23. `backend/verify_scd2_sync.py`

### Setup/Seed Scripts (10 files) - One-time use only
24. `backend/add_comprehensive_scd2_data.py`
25. `backend/add_more_scd_test_data.py`
26. `backend/add_more_test_data.py`
27. `backend/create_multi_table_scd_system.py`
28. `backend/create_rich_scd2_data.py`
29. `backend/setup_comprehensive_test_data.py`
30. `backend/setup_enhanced_test_data.py`
31. `backend/setup_sample_data.py`
32. `backend/setup_scd_test_data.py`
33. `backend/setup_scd2_table.py`

### Cleanup Scripts (3 files) - Already used, can keep one
34. `backend/auto_cleanup_tables.py` - ✅ KEEP (useful utility)
35. `backend/cleanup_all_tables.py` - DELETE (duplicate)

### Guide Files (1 file)
36. `backend/spark_setup_guide.py` - DELETE (not used, Spark is separate)

### Unused Frontend Components (5 files)
37. `frontend/src/components/ConfigForm.tsx` - Not imported in App.tsx
38. `frontend/src/components/EnhancedTableSelector.tsx` - Not used
39. `frontend/src/components/ReportTable.tsx` - Replaced by UltraSimpleReportTable
40. `frontend/src/components/SimpleReportTable.tsx` - Replaced by UltraSimpleReportTable
41. `frontend/src/components/TableSelector.tsx` - Not used

### Unused Frontend Pages (2 files)
42. `frontend/src/pages/ConfigPage.tsx` - Not imported in App.tsx
43. `frontend/src/pages/ReportPage.tsx` - Not imported in App.tsx

### SQL Scripts (2 files)
44. `backend/cleanup_scd2_tables.sql` - Already executed, keep for reference
45. `backend/setup_databases.sql` - ✅ KEEP (useful for setup)

---

## 🔵 OPTIONAL KEEP (Can delete later)

### Docker Files (if not using Docker)
- `docker-compose.yml`
- `Dockerfile.backend`
- `Dockerfile.frontend`
- `Dockerfile.spark`

### Spark Jobs (if not using Spark)
- `spark_jobs/` directory

---

## 📊 SUMMARY

**Total Files to Delete:** 40-45 files
**Estimated Space Saved:** ~500-1000 KB (excluding __pycache__ folders)
**Impact on Product:** ZERO - All deleted files are:
  - Test scripts
  - Unused components
  - Duplicate utilities
  - One-time setup scripts

**Recommended Actions:**
1. Delete test scripts (safe)
2. Delete unused frontend components (safe)
3. Delete duplicate cleanup scripts (safe)
4. Keep one cleanup utility (auto_cleanup_tables.py)
5. Keep setup_databases.sql for reference
6. Optionally delete Docker/Spark files if not needed


