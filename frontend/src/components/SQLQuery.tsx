import React, { useState, useEffect } from "react";

interface SQLQueryProps {
  refreshTrigger: number;
}

interface QueryResult {
  success: boolean;
  data?: any[];
  columns?: string[];
  row_count: number;
  message: string;
  execution_time_ms: number;
}

interface SampleQuery {
  name: string;
  query: string;
  description: string;
}

function SQLQuery({ refreshTrigger }: SQLQueryProps) {
  const [query, setQuery] = useState("SELECT * FROM customers");
  const [database, setDatabase] = useState("target");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [sampleQueries, setSampleQueries] = useState<SampleQuery[]>([]);
  const [sourceInfo, setSourceInfo] = useState<any>(null);
  const [targetInfo, setTargetInfo] = useState<any>(null);

  useEffect(() => {
    fetchSampleQueries();
    fetchDatabaseInfo();
  }, [refreshTrigger]);

  const fetchSampleQueries = async () => {
    try {
      const response = await fetch("http://localhost:8000/sql/sample-queries");
      const data = await response.json();
      setSampleQueries(data.sample_queries);
    } catch (err) {
      console.error("Error fetching sample queries", err);
    }
  };

  const fetchDatabaseInfo = async () => {
    try {
      const [sourceResponse, targetResponse] = await Promise.all([
        fetch("http://localhost:8000/sql/database-info/source"),
        fetch("http://localhost:8000/sql/database-info/target")
      ]);
      
      const sourceData = await sourceResponse.json();
      const targetData = await targetResponse.json();
      
      setSourceInfo(sourceData);
      setTargetInfo(targetData);
    } catch (err) {
      console.error("Error fetching database info", err);
    }
  };

  const executeQuery = async () => {
    if (!query.trim()) {
      setResult({
        success: false,
        row_count: 0,
        message: "Please enter a SQL query",
        execution_time_ms: 0
      });
      return;
    }

    setLoading(true);
    try {
      const response = await fetch("http://localhost:8000/sql/execute-query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: query.trim(),
          database: database,
          limit: 1000
        }),
      });
      
      const data = await response.json();
      setResult(data);
    } catch (err) {
      setResult({
        success: false,
        row_count: 0,
        message: "Network error: Could not execute query",
        execution_time_ms: 0
      });
    } finally {
      setLoading(false);
    }
  };

  const loadSampleQuery = (sampleQuery: SampleQuery) => {
    setQuery(sampleQuery.query);
  };

  return (
    <div className="p-4 bg-white rounded shadow">
      <h2 className="text-lg font-bold mb-4">SQL Query Interface</h2>
      
      {/* Database Info */}
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div className="border rounded p-3">
          <h3 className="font-semibold text-sm mb-2">Source Database</h3>
          {sourceInfo ? (
            <div className="text-xs">
              <p><strong>Database:</strong> {sourceInfo.database_name}</p>
              <p><strong>Host:</strong> {sourceInfo.host}:{sourceInfo.port}</p>
              <p><strong>Status:</strong> 
                <span className={sourceInfo.connection_status === 'Connected' ? 'text-green-600' : 'text-red-600'}>
                  {sourceInfo.connection_status}
                </span>
              </p>
              <p><strong>Tables:</strong> {Object.keys(sourceInfo.tables || {}).join(', ')}</p>
            </div>
          ) : (
            <p className="text-xs text-gray-500">Loading...</p>
          )}
        </div>
        
        <div className="border rounded p-3">
          <h3 className="font-semibold text-sm mb-2">Target Database</h3>
          {targetInfo ? (
            <div className="text-xs">
              <p><strong>Database:</strong> {targetInfo.database_name}</p>
              <p><strong>Host:</strong> {targetInfo.host}:{targetInfo.port}</p>
              <p><strong>Status:</strong> 
                <span className={targetInfo.connection_status === 'Connected' ? 'text-green-600' : 'text-red-600'}>
                  {targetInfo.connection_status}
                </span>
              </p>
              <p><strong>Tables:</strong> {Object.keys(targetInfo.tables || {}).join(', ')}</p>
            </div>
          ) : (
            <p className="text-xs text-gray-500">Loading...</p>
          )}
        </div>
      </div>

      {/* Query Input */}
      <div className="mb-4">
        <div className="flex gap-2 mb-2">
          <label className="text-sm font-medium">Database:</label>
          <select
            value={database}
            onChange={(e) => setDatabase(e.target.value)}
            className="border rounded px-2 py-1 text-sm"
          >
            <option value="source">Source Database</option>
            <option value="target">Target Database</option>
          </select>
        </div>
        
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Enter your SQL query here..."
          className="w-full border rounded p-3 font-mono text-sm"
          rows={4}
        />
        
        <div className="flex gap-2 mt-2">
          <button
            onClick={executeQuery}
            disabled={loading}
            className="bg-blue-600 text-white px-4 py-2 rounded disabled:bg-gray-400"
          >
            {loading ? "Executing..." : "Execute Query"}
          </button>
          
          <button
            onClick={() => setQuery("")}
            className="bg-gray-300 text-gray-700 px-4 py-2 rounded"
          >
            Clear
          </button>
        </div>
      </div>

      {/* Sample Queries */}
      <div className="mb-4">
        <h3 className="font-semibold mb-2">Sample Queries:</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          {sampleQueries.map((sample, idx) => (
            <button
              key={idx}
              onClick={() => loadSampleQuery(sample)}
              className="text-left border rounded p-2 hover:bg-gray-50 text-sm"
              title={sample.description}
            >
              <div className="font-medium text-blue-600">{sample.name}</div>
              <div className="text-xs text-gray-600 font-mono">{sample.query}</div>
            </button>
          ))}
        </div>
      </div>

      {/* Query Results */}
      {result && (
        <div className="border rounded p-4">
          <div className="flex justify-between items-center mb-2">
            <h3 className="font-semibold">Query Results</h3>
            <div className="text-sm text-gray-600">
              Execution time: {result.execution_time_ms}ms
            </div>
          </div>
          
          <div className={`mb-2 p-2 rounded text-sm ${
            result.success ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
          }`}>
            {result.message}
          </div>

          {result.success && result.data && result.data.length > 0 && (
            <div className="overflow-x-auto">
              <table className="table-auto w-full border text-sm">
                <thead>
                  <tr className="bg-gray-100">
                    {result.columns?.map((col, idx) => (
                      <th key={idx} className="border px-2 py-1 text-left">{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.data.map((row, rowIdx) => (
                    <tr key={rowIdx} className={rowIdx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      {result.columns?.map((col, colIdx) => (
                        <td key={colIdx} className="border px-2 py-1">
                          {row[col] !== null ? String(row[col]) : 'NULL'}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {result.success && result.row_count === 0 && (
            <div className="text-gray-600 text-sm">No rows returned.</div>
          )}
        </div>
      )}
    </div>
  );
}

export default SQLQuery;
