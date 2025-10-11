import { useState, useMemo } from "react";
import UltraSimpleReportTable from "../components/UltraSimpleReportTable";

export default function OriginalValidationPage() {
  const [tables, setTables] = useState("ten_million_records");
  const [refreshTrigger, setRefreshTrigger] = useState(0);
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [scdType2Enabled, setScdType2Enabled] = useState(false);
  
  // Database configurations - Updated for Azure SQL
  const [sourceDB, setSourceDB] = useState({
    host: "data-validation-server.database.windows.net",
    port: "1433",
    database: "data-validation-db",
    user: "adminuser",
    password: "Sandy@123"
  });
  
  const [targetDB, setTargetDB] = useState({
    host: "data-validation-server.database.windows.net",
    port: "1433",
    database: "data-validation-db",
    user: "adminuser",
    password: "Sandy@123"
  });
  
  // Connection status
  const [sourceStatus, setSourceStatus] = useState<{status: string, message: string} | null>(null);
  const [targetStatus, setTargetStatus] = useState<{status: string, message: string} | null>(null);
  const [testingSource, setTestingSource] = useState(false);
  const [testingTarget, setTestingTarget] = useState(false);
  
  // Load available tables
  const loadAvailableTables = async () => {
    setLoadingTables(true);
    try {
      const response = await fetch("http://localhost:8000/pyspark/available-tables");
      const data = await response.json();
      if (response.ok) {
        setAvailableTables(data.tables || []);
      }
    } catch (error) {
      console.error("Error loading tables:", error);
    } finally {
      setLoadingTables(false);
    }
  };
  
  // Test source database connection
  const testSourceConnection = async () => {
    setTestingSource(true);
    setSourceStatus(null);
    
    try {
      const response = await fetch("http://localhost:8000/azure-db/test-connection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          host: sourceDB.host,
          port: parseInt(sourceDB.port),
          database: sourceDB.database,
          user: sourceDB.user,
          password: sourceDB.password
        })
      });
      
      const data = await response.json();
      
      if (response.ok && data.message) {
        setSourceStatus({ status: "success", message: "✅ Connected successfully!" });
      } else {
        setSourceStatus({ status: "error", message: `❌ Connection failed: ${data.detail || "Unknown error"}` });
      }
    } catch (error) {
      setSourceStatus({ status: "error", message: `❌ Connection failed: ${error}` });
    } finally {
      setTestingSource(false);
    }
  };
  
  // Test target database connection
  const testTargetConnection = async () => {
    setTestingTarget(true);
    setTargetStatus(null);
    
    try {
      const response = await fetch("http://localhost:8000/azure-db/test-connection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          host: targetDB.host,
          port: parseInt(targetDB.port),
          database: targetDB.database,
          user: targetDB.user,
          password: targetDB.password
        })
      });
      
      const data = await response.json();
      
      if (response.ok && data.message) {
        setTargetStatus({ status: "success", message: "✅ Connected successfully!" });
      } else {
        setTargetStatus({ status: "error", message: `❌ Connection failed: ${data.detail || "Unknown error"}` });
      }
    } catch (error) {
      setTargetStatus({ status: "error", message: `❌ Connection failed: ${error}` });
    } finally {
      setTestingTarget(false);
    }
  };
  
  // Run validation
  const runValidation = async () => {
    setRefreshTrigger(prev => prev + 1);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="container mx-auto px-4 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">
            Data Validation & Sync System
          </h1>
          <p className="text-gray-600">
            Original system with proven reliability
          </p>
        </div>

        {/* Database Connections */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
          {/* Source Database */}
          <div className="p-6 border-2 border-blue-200 rounded-lg">
            <h3 className="text-lg font-semibold text-blue-800 mb-4">
              Source Database
            </h3>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Host
                </label>
                <input
                  type="text"
                  value={sourceDB.host}
                  onChange={(e) => setSourceDB({...sourceDB, host: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Port
                </label>
                <input
                  type="text"
                  value={sourceDB.port}
                  onChange={(e) => setSourceDB({...sourceDB, port: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Database
                </label>
                <input
                  type="text"
                  value={sourceDB.database}
                  onChange={(e) => setSourceDB({...sourceDB, database: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  User
                </label>
                <input
                  type="text"
                  value={sourceDB.user}
                  onChange={(e) => setSourceDB({...sourceDB, user: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Password
                </label>
                <input
                  type="password"
                  value={sourceDB.password}
                  onChange={(e) => setSourceDB({...sourceDB, password: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>

            <button
              onClick={testSourceConnection}
              disabled={testingSource}
              className="w-full mt-4 px-4 py-2 bg-blue-600 text-white font-medium rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              {testingSource ? "Testing..." : "Test Connection"}
            </button>

            {sourceStatus && (
              <div className={`mt-4 p-3 rounded-md ${
                sourceStatus.status === 'success' 
                  ? 'bg-green-100 border-green-500 text-green-700' 
                  : 'bg-red-100 border-red-500 text-red-700'
              }`}>
                {sourceStatus.message}
              </div>
            )}
          </div>

          {/* Target Database */}
          <div className="p-6 border-2 border-green-200 rounded-lg">
            <h3 className="text-lg font-semibold text-green-800 mb-4">
              Target Database
            </h3>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Host
                </label>
                <input
                  type="text"
                  value={targetDB.host}
                  onChange={(e) => setTargetDB({...targetDB, host: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Port
                </label>
                <input
                  type="text"
                  value={targetDB.port}
                  onChange={(e) => setTargetDB({...targetDB, port: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Database
                </label>
                <input
                  type="text"
                  value={targetDB.database}
                  onChange={(e) => setTargetDB({...targetDB, database: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  User
                </label>
                <input
                  type="text"
                  value={targetDB.user}
                  onChange={(e) => setTargetDB({...targetDB, user: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Password
                </label>
                <input
                  type="password"
                  value={targetDB.password}
                  onChange={(e) => setTargetDB({...targetDB, password: e.target.value})}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                />
              </div>
            </div>

            <button
              onClick={testTargetConnection}
              disabled={testingTarget}
              className="w-full mt-4 px-4 py-2 bg-green-600 text-white font-medium rounded-md hover:bg-green-700 disabled:opacity-50"
            >
              {testingTarget ? "Testing..." : "Test Connection"}
            </button>

            {targetStatus && (
              <div className={`mt-4 p-3 rounded-md ${
                targetStatus.status === 'success' 
                  ? 'bg-green-100 border-green-500 text-green-700' 
                  : 'bg-red-100 border-red-500 text-red-700'
              }`}>
                {targetStatus.message}
              </div>
            )}
          </div>
        </div>

        {/* Table Selection */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            Select Tables to Compare
          </h2>
          
          <div className="space-y-4">
            <button
              onClick={loadAvailableTables}
              disabled={loadingTables}
              className="flex items-center px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50"
            >
              {loadingTables ? (
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
              ) : (
                <span className="mr-2">📋</span>
              )}
              Load Available Tables
            </button>

            {availableTables.length > 0 && (
              <div className="mt-4">
                <div className="flex items-center mb-2">
                  <input
                    type="checkbox"
                    id="selectAll"
                    onChange={(e) => {
                      if (e.target.checked) {
                        setTables(availableTables.join(','));
                      } else {
                        setTables('');
                      }
                    }}
                    className="mr-2"
                  />
                  <label htmlFor="selectAll" className="text-sm font-medium text-gray-700">
                    Select All Tables
                  </label>
                </div>
                
                <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                  {availableTables.map((table) => (
                    <div key={table} className="flex items-center">
                      <input
                        type="checkbox"
                        id={table}
                        checked={tables.includes(table)}
                        onChange={(e) => {
                          if (e.target.checked) {
                            setTables(prev => prev ? `${prev},${table}` : table);
                          } else {
                            setTables(prev => prev.split(',').filter(t => t !== table).join(','));
                          }
                        }}
                        className="mr-2"
                      />
                      <label htmlFor={table} className="text-sm text-gray-700">
                        {table}
                      </label>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Or enter manually (comma-separated):
              </label>
              <input
                type="text"
                value={tables}
                onChange={(e) => setTables(e.target.value)}
                placeholder="customers,products,orders"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>
        </div>

        {/* Validation Controls */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-xl font-semibold text-gray-900 mb-2">
                Run Validation
              </h2>
              <p className="text-gray-600">
                Compare data between source and target databases
              </p>
              
              {/* SCD Type 2 Toggle */}
              <div className="mt-4 flex items-center">
                <input
                  type="checkbox"
                  id="scdType2"
                  checked={scdType2Enabled}
                  onChange={(e) => setScdType2Enabled(e.target.checked)}
                  className="mr-2 h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="scdType2" className="text-sm font-medium text-gray-700">
                  Enable SCD Type 2 (Slowly Changing Dimension Type 2)
                </label>
              </div>
              {scdType2Enabled && (
                <div className="mt-2 p-3 bg-blue-50 border border-blue-200 rounded-md">
                  <p className="text-sm text-blue-800">
                    <strong>SCD Type 2:</strong> Historical tracking with effective dates. 
                    Creates new records for changes instead of updating existing ones.
                  </p>
                </div>
              )}
            </div>
            <button
              onClick={runValidation}
              className="flex items-center px-6 py-3 bg-blue-600 text-white font-medium rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <span className="mr-2">🚀</span>
              Run Validation
            </button>
          </div>
        </div>

        {/* Results */}
        <div className="bg-white rounded-lg shadow-md">
          <UltraSimpleReportTable
            selectedTables={useMemo(() => tables.split(',').map(t => t.trim()).filter(t => t), [tables])}
            refreshTrigger={refreshTrigger}
            scdType2Enabled={scdType2Enabled}
          />
        </div>
      </div>
    </div>
  );
}
