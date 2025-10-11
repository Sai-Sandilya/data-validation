import { useState } from "react";
import UltraSimpleReportTable from "../components/UltraSimpleReportTable";

export default function ValidationPage() {
  const [tables, setTables] = useState("ten_million_records");
  const [refreshTrigger, setRefreshTrigger] = useState(0);
  const [showConfig, setShowConfig] = useState(false);
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  
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
      const response = await fetch("http://localhost:8000/compare/available-tables");
      const data = await response.json();
      if (response.ok) {
        setAvailableTables(data.common_tables || []);
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
      const response = await fetch("http://localhost:8000/db/test-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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
      const response = await fetch("http://localhost:8000/db/test-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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

  const runValidation = async () => {
    setRefreshTrigger(prev => prev + 1);
  };

  return (
    <div className="min-h-screen bg-gray-100 py-8">
      <div className="max-w-6xl mx-auto px-4">
        <div className="bg-white rounded-xl shadow-lg p-8">
          <div className="text-center mb-8">
            <h1 className="text-3xl font-bold text-gray-800 mb-2">🔍 Data Validation & Sync</h1>
            <p className="text-gray-600">Compare tables between source and target databases, then sync differences</p>
          </div>

          {/* Database Configuration Section */}
          <div className="mb-8 border-b pb-6">
            <button
              onClick={() => setShowConfig(!showConfig)}
              className="flex items-center justify-between w-full px-4 py-3 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors"
            >
              <div className="flex items-center gap-2">
                <span className="text-lg">🔌 Database Connections</span>
                <span className="text-sm text-gray-600">
                  (Source: {sourceDB.database} | Target: {targetDB.database})
                </span>
              </div>
              <span className="text-2xl">{showConfig ? "▼" : "▶"}</span>
            </button>

            {showConfig && (
              <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Source Database */}
                <div className="border rounded-lg p-4 bg-blue-50">
                  <h3 className="font-bold text-lg mb-3 text-blue-800">📤 Source Database</h3>
                  <div className="space-y-3">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Host</label>
                      <input
                        value={sourceDB.host}
                        onChange={(e) => setSourceDB({...sourceDB, host: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-blue-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Port</label>
                      <input
                        value={sourceDB.port}
                        onChange={(e) => setSourceDB({...sourceDB, port: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-blue-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Database</label>
                      <input
                        value={sourceDB.database}
                        onChange={(e) => setSourceDB({...sourceDB, database: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-blue-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">User</label>
                      <input
                        value={sourceDB.user}
                        onChange={(e) => setSourceDB({...sourceDB, user: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-blue-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
                      <input
                        type="password"
                        value={sourceDB.password}
                        onChange={(e) => setSourceDB({...sourceDB, password: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-blue-500"
                      />
                    </div>

                    {/* Test Connection Button */}
                    <button
                      onClick={testSourceConnection}
                      disabled={testingSource}
                      className={`w-full px-4 py-2 rounded font-medium transition-colors ${
                        testingSource 
                          ? "bg-gray-300 cursor-not-allowed" 
                          : "bg-blue-600 hover:bg-blue-700 text-white"
                      }`}
                    >
                      {testingSource ? "🔄 Testing..." : "🔌 Test Connection"}
                    </button>

                    {/* Connection Status */}
                    {sourceStatus && (
                      <div className={`p-3 rounded ${
                        sourceStatus.status === "success" 
                          ? "bg-green-100 border border-green-400 text-green-800" 
                          : "bg-red-100 border border-red-400 text-red-800"
                      }`}>
                        {sourceStatus.message}
                      </div>
                    )}
                  </div>
                </div>

                {/* Target Database */}
                <div className="border rounded-lg p-4 bg-green-50">
                  <h3 className="font-bold text-lg mb-3 text-green-800">📥 Target Database</h3>
                  <div className="space-y-3">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Host</label>
                      <input
                        value={targetDB.host}
                        onChange={(e) => setTargetDB({...targetDB, host: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-green-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Port</label>
                      <input
                        value={targetDB.port}
                        onChange={(e) => setTargetDB({...targetDB, port: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-green-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Database</label>
                      <input
                        value={targetDB.database}
                        onChange={(e) => setTargetDB({...targetDB, database: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-green-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">User</label>
                      <input
                        value={targetDB.user}
                        onChange={(e) => setTargetDB({...targetDB, user: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-green-500"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
                      <input
                        type="password"
                        value={targetDB.password}
                        onChange={(e) => setTargetDB({...targetDB, password: e.target.value})}
                        className="w-full px-3 py-2 border border-gray-300 rounded focus:ring-2 focus:ring-green-500"
                      />
                    </div>

                    {/* Test Connection Button */}
                    <button
                      onClick={testTargetConnection}
                      disabled={testingTarget}
                      className={`w-full px-4 py-2 rounded font-medium transition-colors ${
                        testingTarget 
                          ? "bg-gray-300 cursor-not-allowed" 
                          : "bg-green-600 hover:bg-green-700 text-white"
                      }`}
                    >
                      {testingTarget ? "🔄 Testing..." : "🔌 Test Connection"}
                    </button>

                    {/* Connection Status */}
                    {targetStatus && (
                      <div className={`p-3 rounded ${
                        targetStatus.status === "success" 
                          ? "bg-green-100 border border-green-400 text-green-800" 
                          : "bg-red-100 border border-red-400 text-red-800"
                      }`}>
                        {targetStatus.message}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
          
          <div className="max-w-2xl mx-auto mb-8">
            <div className="mb-4">
              <label className="block text-lg font-medium text-gray-700 mb-3">
                📋 Select Tables to Compare:
              </label>
              
              {/* Load Tables Button */}
              <div className="mb-3">
                <button
                  onClick={loadAvailableTables}
                  disabled={loadingTables}
                  className="bg-green-500 text-white px-4 py-2 rounded hover:bg-green-600 disabled:bg-gray-400 text-sm"
                >
                  {loadingTables ? "🔄 Loading..." : "📋 Load Available Tables"}
                </button>
                {availableTables.length > 0 && (
                  <span className="ml-3 text-sm text-green-600">
                    ✅ Found {availableTables.length} common tables
                  </span>
                )}
              </div>
              
              {/* Table Selection */}
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="select-all"
                    checked={availableTables.length > 0 && tables.split(',').length === availableTables.length}
                    onChange={(e) => {
                      if (e.target.checked) {
                        setTables(availableTables.join(','));
                      } else {
                        setTables('');
                      }
                    }}
                    className="w-4 h-4"
                  />
                  <label htmlFor="select-all" className="text-sm font-medium text-gray-700">
                    Select All Tables
                  </label>
                </div>
                
                <div className="grid grid-cols-2 gap-2 max-h-40 overflow-y-auto border rounded p-2 bg-gray-50">
                  {availableTables.map((table) => (
                    <div key={table} className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        id={`table-${table}`}
                        checked={tables.split(',').includes(table)}
                        onChange={(e) => {
                          const currentTables = tables.split(',').filter(t => t.trim());
                          if (e.target.checked) {
                            setTables([...currentTables, table].join(','));
                          } else {
                            setTables(currentTables.filter(t => t !== table).join(','));
                          }
                        }}
                        className="w-4 h-4"
                      />
                      <label htmlFor={`table-${table}`} className="text-sm font-mono text-gray-700">
                        {table}
                      </label>
                    </div>
                  ))}
                </div>
              </div>
              
              {/* Manual Input (Fallback) */}
              <div className="mt-4">
                <label className="block text-sm font-medium text-gray-600 mb-2">
                  Or enter manually (comma-separated):
                </label>
      <input
        value={tables}
        onChange={(e) => setTables(e.target.value)}
                  className="w-full text-sm border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="customers,orders,products"
                />
              </div>
            </div>

            <div className="text-center">
      <button
        onClick={runValidation}
                className="bg-blue-600 text-white px-8 py-4 rounded-lg hover:bg-blue-700 transition-colors font-semibold text-lg shadow-md"
      >
                🚀 Run Validation
      </button>
            </div>
          </div>

          <UltraSimpleReportTable 
            refreshTrigger={refreshTrigger} 
            selectedTables={tables.split(",").map(t => t.trim()).filter(t => t.length > 0)}
          />
        </div>
      </div>
    </div>
  );
}
