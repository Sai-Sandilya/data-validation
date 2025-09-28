import React, { useState } from "react";
import ConfigForm from "./components/ConfigForm";
import TableSelector from "./components/TableSelector";
import ReportTable from "./components/ReportTable";
import SQLQuery from "./components/SQLQuery";

function App() {
  const [refreshCount, setRefreshCount] = useState(0);
  const [activeTab, setActiveTab] = useState("validation");

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <h1 className="text-2xl font-bold text-center mb-6">
        Data Validation & Sync System
      </h1>

      {/* Tab Navigation */}
      <div className="max-w-6xl mx-auto mb-6">
        <div className="flex border-b">
          <button
            onClick={() => setActiveTab("validation")}
            className={`px-4 py-2 font-medium ${
              activeTab === "validation"
                ? "border-b-2 border-blue-500 text-blue-600"
                : "text-gray-600 hover:text-gray-800"
            }`}
          >
            Data Validation & Sync
          </button>
          <button
            onClick={() => setActiveTab("sql")}
            className={`px-4 py-2 font-medium ${
              activeTab === "sql"
                ? "border-b-2 border-blue-500 text-blue-600"
                : "text-gray-600 hover:text-gray-800"
            }`}
          >
            SQL Query Interface
          </button>
        </div>
      </div>

      {/* Tab Content */}
      <div className="max-w-6xl mx-auto">
        {activeTab === "validation" && (
          <div className="space-y-6">
            <ConfigForm />
            <TableSelector onValidationComplete={() => setRefreshCount(refreshCount + 1)} />
            <ReportTable refreshTrigger={refreshCount} />
          </div>
        )}

        {activeTab === "sql" && (
          <div>
            <div className="bg-white rounded-lg shadow p-4 mb-4">
              <div className="text-sm text-gray-600">
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
            <SQLQuery refreshTrigger={refreshCount} />
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
