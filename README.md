# Data Validation & SCD Type 2 Sync System

A comprehensive **Slowly Changing Dimension (SCD) Type 2** data validation and synchronization system with full historical tracking capabilities.

![SCD Type 2 Demo](https://img.shields.io/badge/SCD-Type%202-blue) ![React](https://img.shields.io/badge/React-Frontend-61dafb) ![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688) ![MySQL](https://img.shields.io/badge/MySQL-Database-4479A1)

## 🚀 Features

### ✅ TRUE SCD Type 2 Implementation
- **Historical Tracking**: Multiple rows per business entity with complete timeline
- **Surrogate Keys**: Auto-increment primary keys for proper versioning
- **Effective Dating**: `effective_date`, `end_date`, and `is_current` flags
- **Career Progressions**: Track salary changes, promotions, relocations

### ✅ Dual SCD Support
- **SCD Type 2**: Historical tracking with new row creation
- **SCD Type 3**: Previous value storage in `*_previous` columns
- **Smart Detection**: Automatic table type recognition

### ✅ Advanced Comparison Engine
- **Hash-Based Comparison**: Efficient record matching
- **System Column Exclusion**: Ignores timestamps and auto-generated fields  
- **Business Key Matching**: Compares by `business_id` for SCD2 tables
- **Detailed Diff Reports**: Shows exact field-by-field changes

### ✅ Frontend Dashboard
- **React + TypeScript**: Modern responsive UI
- **Table Selection**: Easy switching between SCD2 and SCD3 tables
- **Interactive Sync**: Select specific records to synchronize
- **Real-time Updates**: Live validation status and progress
- **Download Reports**: Export validation and sync results as CSV/JSON

### ✅ Comprehensive Testing Data
- **Career Progressions**: Junior → Senior → Principal → VP → CTO journeys
- **Geographic Moves**: NY → Paris → London relocations  
- **Industry Scenarios**: Technology, Finance, Consulting, Startups
- **Edge Cases**: New hires, legacy employees, missing records

## 🏗️ Architecture

### Backend (FastAPI + Python)
```
backend/
├── main.py                 # FastAPI application entry point
├── routes/
│   ├── comparison_routes.py    # SCD2/SCD3 comparison logic
│   ├── audit_routes.py         # Sync audit and logging
│   └── validation_routes.py    # Data validation endpoints
├── config/
│   ├── db_config.py           # Database connections
│   └── settings.py            # System settings and audit columns
└── services/
    ├── validation_service.py  # Core validation logic
    └── sync_service.py        # Synchronization engine
```

### Frontend (React + Vite)
```
frontend/
├── src/
│   ├── components/
│   │   ├── ReportTable.tsx    # Main validation/sync interface
│   │   └── ConfigForm.tsx     # Configuration management
│   ├── pages/
│   │   ├── ValidationPage.tsx # SCD validation dashboard
│   │   └── ReportPage.tsx     # Sync reports and downloads
│   └── api/
│       └── endpoints.ts       # Backend API integration
```

## 🚀 Quick Start

### Prerequisites
- **Python 3.11+**
- **Node.js 18+**
- **MySQL 8.0+**

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/Sai-Sandilya/data-validation.git
cd data-validation
```

2. **Backend Setup**
```bash
cd backend
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

3. **Database Configuration**
```python
# backend/config/db_config.py
SOURCE_DB = {
    "host": "localhost",
    "port": 3306,
    "database": "source_db",
    "user": "your_username",
    "password": "your_password"
}

TARGET_DB = {
    "host": "localhost", 
    "port": 3306,
    "database": "target_db",
    "user": "your_username",
    "password": "your_password"
}
```

4. **Frontend Setup**
```bash
cd ../frontend
npm install
```

5. **Create Test Data**
```bash
cd ../backend
python create_rich_scd2_data.py
```

### Running the Application

1. **Start Backend**
```bash
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

2. **Start Frontend**
```bash
cd frontend
npm run dev
```

3. **Access Application**
- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

## 📊 SCD Type 2 Demo Data

### Career Progression Examples

**Emma's Journey (business_id: 200)**
```sql
Step 1: Junior-Developer     | $65,000  | 2024-09-28 → 2025-04-01 | EXPIRED
Step 2: Mid-Developer        | $80,000  | 2025-04-01 → 2025-06-30 | EXPIRED  
Step 3: Senior-Developer     | $100,000 | 2025-06-30 → 2025-09-28 | EXPIRED
Step 4: Senior-Architect     | $135,000 | 2025-09-28 → 9999-12-31 | CURRENT ✅
```

**Global Expansion (Ava - business_id: 210)**
```sql
Step 1: Regional-Manager (NY)     | $85,000  | EXPIRED
Step 2: Senior-Manager (Paris)    | $95,000  | EXPIRED
Step 3: Global-Director (London)  | $120,000 | CURRENT ✅
```

## 🎯 Testing Scenarios

### 1. Technology Company Career Ladders
- **Emma**: Junior → Mid → Senior → Senior-Architect ($65K → $135K)
- **Liam**: Junior → Senior → Principal-Engineer ($70K → $140K)  
- **Olivia**: Team-Lead → VP-Technology ($110K → $180K)
- **Noah**: Engineering-Director → CTO-Chief ($180K → $250K)

### 2. Global Consulting Expansion
- **Ava**: NY → Paris → London ($85K → $120K)
- **William**: Local-Consultant → Regional-VP ($75K → $115K)
- **Sophia**: Singapore → Tokyo ($80K → $110K)

### 3. Finance Sector Growth
- **James**: Analyst → Senior-Analyst → Investment-Director ($90K → $200K)
- **Isabella**: Trader-Associate → Portfolio-Manager ($100K → $160K)

### 4. Startup Ecosystem
- **Mia**: Technical-Lead → Co-Founder ($150K → $300K + Equity)
- **Ethan**: Full-Stack → Lead-Developer ($110K → $180K)

## 🔍 Key SQL Queries

### Get Current Record for Business Entity
```sql
SELECT * FROM customers_scd2 
WHERE business_id = 200 AND is_current = TRUE;
```

### View Complete Career Timeline
```sql
SELECT business_id, name, status, amount, 
       effective_date, end_date, is_current
FROM customers_scd2 
WHERE business_id = 200 
ORDER BY effective_date;
```

### Find Multi-Version Entities
```sql
SELECT business_id, COUNT(*) as versions
FROM customers_scd2 
GROUP BY business_id
HAVING COUNT(*) > 1
ORDER BY versions DESC;
```

## 🛠️ API Endpoints

### Validation & Comparison
- `POST /compare/compare-table` - Compare source vs target tables
- `POST /compare/detailed-comparison` - Get field-by-field differences
- `POST /compare/sync-selected-records` - Sync specific records

### Audit & Reports  
- `GET /audit/sync-logs` - Get sync history
- `GET /audit/download-csv` - Download CSV reports
- `GET /audit/download-json` - Download JSON reports

### Configuration
- `GET /tables` - List available tables
- `POST /validate-connection` - Test database connectivity

## 📈 System Statistics

- **69 Files**: Complete full-stack implementation
- **10,397 Lines of Code**: Comprehensive feature set
- **16 Source Records**: Current career positions
- **37 Target Records**: Including historical data
- **19 Business Entities**: Unique employees/companies
- **18 Historical Records**: Complete career timelines

## 🧪 Testing

### Run Test Data Creation
```bash
# Comprehensive SCD2 test scenarios
python backend/create_rich_scd2_data.py

# Verify current records  
python backend/show_current_scd2_records.py

# Check career progressions
python backend/verify_all_careers.py
```

### Frontend Testing
1. Select "customers_scd2 (SCD Type 2)" table
2. Click "Validate Selected Tables"
3. View detailed career progressions
4. Select records and click "Sync Selected"
5. Watch TRUE SCD2 magic happen! ✨

## 📚 Documentation

### SCD Type 2 Best Practices
- **Always use `is_current = TRUE`** to identify latest records
- **Never rely on `surrogate_id`** for chronological ordering
- **Use `effective_date`** for point-in-time queries
- **Validate data integrity** with unique current record checks

### Hash Comparison Logic
- Excludes system columns: `surrogate_id`, `effective_date`, `end_date`, `is_current`
- Focuses on business data changes only
- Enables accurate change detection

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🎉 Acknowledgments

- Built with ❤️ for data engineers and database professionals
- Implements industry-standard SCD Type 2 patterns
- Production-ready for enterprise data warehousing

---

**Ready to experience TRUE SCD Type 2?** 🚀

Visit the [live demo](http://localhost:5173) and explore comprehensive historical data tracking!
