import React, { useState } from "react";
import SQLQuery from "../components/SQLQuery";

function SQLQueryPage() {
  const [refreshTrigger, setRefreshTrigger] = useState(0);

  const handleRefresh = () => {
    setRefreshTrigger(prev => prev + 1);
  };

  return (
    <div className="min-h-screen bg-gray-100 p-4">
      <div className="max-w-6xl mx-auto">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">SQL Query Interface</h1>
          <button
            onClick={handleRefresh}
            className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700"
          >
            Refresh
          </button>
        </div>
        
        <div className="bg-white rounded-lg shadow p-4 mb-4">
          <div className="text-sm text-gray-600 mb-4">
            <p><strong>💡 Pro Tip:</strong> Use this interface to directly query your databases and verify sync results!</p>
            <p><strong>🔍 Common Use Cases:</strong></p>
            <ul className="list-disc list-inside ml-4 mt-2">
              <li>Check if records were synced: <code className="bg-gray-100 px-1 rounded">SELECT * FROM customers WHERE record_status = 'UPDATED'</code></li>
              <li>Compare record counts: Run <code className="bg-gray-100 px-1 rounded">SELECT COUNT(*) FROM customers</code> on both databases</li>
              <li>Verify specific records: <code className="bg-gray-100 px-1 rounded">SELECT * FROM customers WHERE id = 1</code></li>
              <li>Check table structure: <code className="bg-gray-100 px-1 rounded">DESCRIBE customers</code></li>
            </ul>
          </div>
        </div>

        <SQLQuery refreshTrigger={refreshTrigger} />
      </div>
    </div>
  );
}

export default SQLQueryPage;
