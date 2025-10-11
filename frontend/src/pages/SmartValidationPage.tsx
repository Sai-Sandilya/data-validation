import { useState } from "react";
import SmartConnectionForm from "../components/SmartConnectionForm";
import UltraSimpleReportTable from "../components/UltraSimpleReportTable";

interface ConnectionData {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  [key: string]: any; // For additional cloud fields
}

export default function SmartValidationPage() {
  const [tables, setTables] = useState("customers");
  const [refreshTrigger, setRefreshTrigger] = useState(0);
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  
  // Smart database configurations with cloud support
  const [sourceDB, setSourceDB] = useState<ConnectionData>({
    host: "localhost",
    port: 3306,
    database: "source_db",
    user: "root",
    password: "Sandy@123"
  });
  
  const [targetDB, setTargetDB] = useState<ConnectionData>({
    host: "localhost",
    port: 3306,
    database: "target_db",
    user: "root",
    password: "Sandy@123"
  });
  
  // Connection status
  const [sourceStatus, setSourceStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [targetStatus, setTargetStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [sourceMessage, setSourceMessage] = useState('');
  const [targetMessage, setTargetMessage] = useState('');
  
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
    setSourceStatus('testing');
    setSourceMessage('Testing connection...');
    
    try {
      // Try cloud detection first
      const response = await fetch('http://localhost:8000/cloud-detection/test-connection', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(sourceDB)
      });
      
      if (response.ok) {
        const result = await response.json();
        if (result.success) {
          setSourceStatus('success');
          setSourceMessage(`Connected successfully! Found ${result.tables_count} tables.`);
          return;
        }
      }
      
      // Fallback to original endpoint
      const fallbackResponse = await fetch('http://localhost:8000/db/test-connection', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          host: sourceDB.host,
          port: sourceDB.port,
          database: sourceDB.database,
          user: sourceDB.user,
          password: sourceDB.password
        })
      });
      
      const data = await fallbackResponse.json();
      
      if (fallbackResponse.ok && data.message) {
        setSourceStatus('success');
        setSourceMessage('✅ Connected successfully!');
      } else {
        setSourceStatus('error');
        setSourceMessage(`❌ Connection failed: ${data.detail || 'Unknown error'}`);
      }
    } catch (error) {
      setSourceStatus('error');
      setSourceMessage('Connection test failed');
    }
  };
  
  // Test target database connection
  const testTargetConnection = async () => {
    setTargetStatus('testing');
    setTargetMessage('Testing connection...');
    
    try {
      // Try cloud detection first
      const response = await fetch('http://localhost:8000/cloud-detection/test-connection', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(targetDB)
      });
      
      if (response.ok) {
        const result = await response.json();
        if (result.success) {
          setTargetStatus('success');
          setTargetMessage(`Connected successfully! Found ${result.tables_count} tables.`);
          return;
        }
      }
      
      // Fallback to original endpoint
      const fallbackResponse = await fetch('http://localhost:8000/db/test-connection', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          host: targetDB.host,
          port: targetDB.port,
          database: targetDB.database,
          user: targetDB.user,
          password: targetDB.password
        })
      });
      
      const data = await fallbackResponse.json();
      
      if (fallbackResponse.ok && data.message) {
        setTargetStatus('success');
        setTargetMessage('✅ Connected successfully!');
      } else {
        setTargetStatus('error');
        setTargetMessage(`❌ Connection failed: ${data.detail || 'Unknown error'}`);
      }
    } catch (error) {
      setTargetStatus('error');
      setTargetMessage('Connection test failed');
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
            Smart Data Validation & Sync System
          </h1>
          <p className="text-gray-600">
            Universal database support with automatic cloud detection
          </p>
        </div>

        {/* Smart Database Connections */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
          <SmartConnectionForm
            title="Source Database"
            connectionData={sourceDB}
            onConnectionDataChange={setSourceDB}
            onTestConnection={testSourceConnection}
            connectionStatus={sourceStatus}
            connectionMessage={sourceMessage}
            color="blue"
          />
          
          <SmartConnectionForm
            title="Target Database"
            connectionData={targetDB}
            onConnectionDataChange={setTargetDB}
            onTestConnection={testTargetConnection}
            connectionStatus={targetStatus}
            connectionMessage={targetMessage}
            color="green"
          />
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
            selectedTables={tables.split(',').map(t => t.trim()).filter(t => t)}
            refreshTrigger={refreshTrigger}
          />
        </div>
      </div>
    </div>
  );
}
