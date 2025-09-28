import React, { useState, useEffect } from "react";

interface ReportTableProps {
  refreshTrigger: number;
}

interface DetailedRecord {
  record_id: any;
  source_data?: any;
  target_data?: any;
  source_hash?: string;
  target_hash?: string;
  difference_type: string;
  differences: string[];
}

function ReportTable({ refreshTrigger }: ReportTableProps) {
  const [report, setReport] = useState<any[]>([]);
  const [syncMessage, setSyncMessage] = useState("");
  const [showDetails, setShowDetails] = useState<string | null>(null);
  const [detailedData, setDetailedData] = useState<DetailedRecord[]>([]);
  const [selectedRecords, setSelectedRecords] = useState<Set<any>>(new Set());
  const [lastSyncId, setLastSyncId] = useState<string | null>(null);
  const [isDownloading, setIsDownloading] = useState(false);
  const [scdConfig, setScdConfig] = useState<{[key: string]: 'SCD2' | 'SCD3'}>({
    customers: 'SCD3', // Default configuration - SCD Type 3
    customers_scd2: 'SCD2' // SCD Type 2 table
  });
  const [activeTable, setActiveTable] = useState<string>('customers'); // Currently selected table

  const fetchReport = async () => {
    try {
      // Get real comparison data for the active table
      const response = await fetch("http://localhost:8000/compare/compare-table", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table: activeTable }),
      });
      const data = await response.json();
      
      // Create a comprehensive validation audit log with detailed records
      if (data.matched_count || data.unmatched_count || data.missing_in_target || data.extra_in_target) {
        try {
          // First get detailed comparison data for audit
          const detailedResponse = await fetch("http://localhost:8000/compare/detailed-comparison", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ table: activeTable }),
          });
          
          const validationSyncId = `VALIDATION_${new Date().toISOString().replace(/[-:.]/g, '').replace('T', '_').slice(0, 17)}`;
          
          if (detailedResponse.ok) {
            const detailedData = await detailedResponse.json();
            
            // Create detailed audit records for CSV download
            const detailedAuditRecords = detailedData.records.map((record: any) => ({
              sync_id: validationSyncId,
              record_id: record.record_id,
              operation_type: record.difference_type, // 'missing', 'mismatch', 'extra'
              old_values: record.target_data,
              new_values: record.source_data,
              changes_made: record.differences,
              error_message: null
            }));
            
            // Create the main audit log
            const auditResponse = await fetch("http://localhost:8000/audit/create-validation-audit", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                sync_id: validationSyncId,
                table_name: activeTable,
                sync_timestamp: new Date().toISOString(),
                total_source_records: data.source_count || 0,
                total_target_records: data.target_count || 0,
                matched_count: data.matched_count || 0,
                unmatched_count: data.unmatched_count || 0,
                missing_count: data.missing_in_target || 0,
                extra_count: data.extra_in_target || 0,
                synced_records: [],
                processing_time_seconds: 0,
                user_id: "validation_user",
                status: "validation_complete",
                detailed_records: detailedAuditRecords
              }),
            });
            
            if (auditResponse.ok) {
              const auditData = await auditResponse.json();
              setLastSyncId(auditData.sync_id);
              console.log("Created detailed validation audit log:", auditData.sync_id);
            }
          } else {
            // Fallback to basic audit if detailed fails
            const auditResponse = await fetch("http://localhost:8000/audit/create-sync-log", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                sync_id: validationSyncId,
                table_name: activeTable,
                sync_timestamp: new Date().toISOString(),
                total_source_records: data.source_count || 0,
                total_target_records: data.target_count || 0,
                matched_count: data.matched_count || 0,
                unmatched_count: data.unmatched_count || 0,
                missing_count: data.missing_in_target || 0,
                extra_count: data.extra_in_target || 0,
                synced_records: [],
                processing_time_seconds: 0,
                user_id: "validation_user",
                status: "validation_complete"
              }),
            });
            
            if (auditResponse.ok) {
              const auditData = await auditResponse.json();
              setLastSyncId(auditData.sync_id);
              console.log("Created basic validation audit log:", auditData.sync_id);
            }
          }
        } catch (auditErr) {
          console.warn("Failed to create validation audit log:", auditErr);
        }
      }
      
      setReport([{
        source_table: activeTable,
        target_table: activeTable,
        matched: data.matched_count,
        unmatched: data.unmatched_count,
        missing_in_target: data.missing_in_target,
        extra_in_target: data.extra_in_target,
        sync_status: data.unmatched_count > 0 ? "Pending" : "Synced"
      }]);
    } catch (err) {
      console.error("Error fetching report", err);
      // Show mock data if API fails
      setReport([
        {
          source_table: "customers",
          target_table: "customers",
          matched: 2,
          unmatched: 1,
          missing_in_target: 1,
          extra_in_target: 0,
          sync_status: "Pending"
        }
      ]);
    }
  };

  useEffect(() => {
    fetchReport();
  }, [refreshTrigger, activeTable]); // Also refresh when active table changes

  const fetchDetailedData = async (table: string) => {
    try {
      const response = await fetch("http://localhost:8000/compare/detailed-comparison", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table }),
      });
      const data = await response.json();
      setDetailedData(data.records);
      setShowDetails(table);
    } catch (err) {
      console.error("Error fetching detailed data", err);
    }
  };

  const handleSync = async (table: string) => {
    if (selectedRecords.size === 0) {
      setSyncMessage("Please select records to sync");
      return;
    }

    try {
      setSyncMessage("Syncing selected records...");
      
      const response = await fetch("http://localhost:8000/compare/sync-selected-records", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ 
          table: activeTable, // Use the active table 
          selected_records: Array.from(selectedRecords),
          scd_type: scdConfig[activeTable] || 'SCD3' // Pass SCD strategy for active table
        }),
      });
      const data = await response.json();
      
      if (response.ok) {
        setSyncMessage(`${data.message} (${data.synced_count} records synced)`);
        setLastSyncId(data.sync_id); // Store sync ID for download
        setSelectedRecords(new Set());
        setShowDetails(null); // Close the modal
        
        // Wait a moment for database to commit, then refresh
        setTimeout(() => {
          fetchReport(); // ✅ Refresh after sync with delay
        }, 1000);
      } else {
        setSyncMessage(`Sync failed: ${data.detail || 'Unknown error'}`);
      }
    } catch (err) {
      setSyncMessage("Error syncing table");
      console.error("Sync error:", err);
    }
  };

  const toggleRecordSelection = (recordId: any) => {
    const newSelected = new Set(selectedRecords);
    if (newSelected.has(recordId)) {
      newSelected.delete(recordId);
    } else {
      newSelected.add(recordId);
    }
    setSelectedRecords(newSelected);
  };

  const selectAllRecords = () => {
    const allRecordIds = detailedData.map(record => record.record_id);
    setSelectedRecords(new Set(allRecordIds));
  };

  const clearAllSelection = () => {
    setSelectedRecords(new Set());
  };

  const downloadSyncReport = async (format: string = 'csv') => {
    setIsDownloading(true);
    let syncId = lastSyncId;
    
    try {
      // If no lastSyncId, get the most recent one
      if (!syncId) {
        console.log("No lastSyncId found, fetching most recent sync log...");
        const logsResponse = await fetch(`http://localhost:8000/audit/sync-logs?limit=1&table_name=${activeTable}`);
        if (!logsResponse.ok) {
          throw new Error(`Failed to fetch sync logs: ${logsResponse.status} ${logsResponse.statusText}`);
        }
        const logsData = await logsResponse.json();
        
        if (logsData.logs && logsData.logs.length > 0) {
          syncId = logsData.logs[0].sync_id;
          console.log("Using most recent sync ID:", syncId);
          setLastSyncId(syncId); // Store it for future downloads
        } else {
          setSyncMessage("No sync logs available to download. Please run a validation first.");
          return;
        }
      }

      console.log(`Downloading sync report for ID: ${syncId}, format: ${format}`);
      const response = await fetch(`http://localhost:8000/audit/download-sync-report/${syncId}?format=${format}`);
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error("Download API error:", response.status, response.statusText, errorText);
        throw new Error(`Download failed: ${response.status} ${response.statusText}. ${errorText}`);
      }
      
      const data = await response.json();
      console.log("Download response:", data);
      
      if (data.content) {
        // Decode base64 content and trigger download
        const byteCharacters = atob(data.content);
        const byteNumbers = new Array(byteCharacters.length);
        for (let i = 0; i < byteCharacters.length; i++) {
          byteNumbers[i] = byteCharacters.charCodeAt(i);
        }
        const byteArray = new Uint8Array(byteNumbers);
        const blob = new Blob([byteArray], { type: data.content_type });
        
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = data.filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
        
        setSyncMessage(`Downloaded ${data.filename} successfully`);
      } else {
        setSyncMessage("No content received from server");
      }
    } catch (err) {
      console.error("Download error:", err);
      const errorMessage = err instanceof Error ? err.message : String(err);
      setSyncMessage(`Error downloading report: ${errorMessage}`);
    } finally {
      setIsDownloading(false);
    }
  };

  return (
    <div className="p-4 bg-white rounded shadow">
      {/* SCD Configuration Section */}
      <div className="mb-6 p-4 bg-blue-50 rounded-lg border">
        <h3 className="text-md font-semibold mb-3 text-blue-800">🔧 SCD Strategy Configuration</h3>
        <div className="space-y-3">
          <div className="text-sm text-blue-700 mb-3">
            Configure how each table should handle data changes during sync operations:
          </div>
          
          {/* Table Configuration Rows */}
          {Object.keys(scdConfig).map((tableName) => (
            <div key={tableName} className="flex items-center justify-between bg-white p-3 rounded border">
              <div className="flex items-center gap-3">
                <span className="font-medium text-gray-800">📋 Table: {tableName}</span>
                <div className="text-xs text-gray-600">
                  Current strategy: <span className="font-semibold">{scdConfig[tableName]}</span>
                </div>
              </div>
              
              <div className="flex gap-2">
                <select
                  value={scdConfig[tableName]}
                  onChange={(e) => setScdConfig(prev => ({
                    ...prev,
                    [tableName]: e.target.value as 'SCD2' | 'SCD3'
                  }))}
                  className="px-3 py-1 border rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="SCD2">SCD Type 2</option>
                  <option value="SCD3">SCD Type 3</option>
                </select>
                
                <div className="text-xs text-gray-500 max-w-xs">
                  {scdConfig[tableName] === 'SCD2' ? 
                    '📅 Keep history with effective dates' : 
                    '🔄 Store previous values in separate columns'
                  }
                </div>
              </div>
            </div>
          ))}
          
          {/* SCD Type Explanations */}
          <div className="grid grid-cols-2 gap-4 mt-4">
            <div className="bg-green-50 p-3 rounded border border-green-200">
              <h4 className="font-semibold text-green-800 mb-2">📅 SCD Type 2 - Historical Tracking</h4>
              <ul className="text-xs text-green-700 space-y-1">
                <li>• Creates new rows for changes</li>
                <li>• Maintains full history</li>
                <li>• Uses effective_date & end_date</li>
                <li>• Best for: Customer data, pricing</li>
              </ul>
            </div>
            
            <div className="bg-orange-50 p-3 rounded border border-orange-200">
              <h4 className="font-semibold text-orange-800 mb-2">🔄 SCD Type 3 - Previous Value</h4>
              <ul className="text-xs text-orange-700 space-y-1">
                <li>• Updates existing rows</li>
                <li>• Keeps previous value in *_previous columns</li>
                <li>• Limited history (before/after)</li>
                <li>• Best for: Product info, configurations</li>
              </ul>
            </div>
          </div>
        </div>
      </div>

      {/* Table Selector */}
      <div className="mb-6 p-4 bg-gray-50 rounded-lg border">
        <h3 className="text-md font-semibold mb-3 text-gray-800">📋 Active Table Selection</h3>
        <div className="flex items-center gap-4">
          <span className="text-sm font-medium text-gray-700">Select table to validate and sync:</span>
          <select
            value={activeTable}
            onChange={(e) => setActiveTable(e.target.value)}
            className="px-4 py-2 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
          >
            <option value="customers">📊 customers (SCD Type 3)</option>
            <option value="customers_scd2">🔄 customers_scd2 (SCD Type 2)</option>
          </select>
          <div className="text-xs text-gray-600 bg-blue-50 px-3 py-1 rounded border">
            <strong>Current:</strong> {activeTable} | <strong>Strategy:</strong> {scdConfig[activeTable]}
          </div>
        </div>
      </div>

      <div className="flex justify-between items-center mb-4">
        <h2 className="text-lg font-bold">Validation Report - {activeTable}</h2>
        <div className="flex gap-2">
          {lastSyncId && (
            <>
              <button
                onClick={() => downloadSyncReport('csv')}
                disabled={isDownloading}
                className="bg-green-600 text-white px-3 py-1 rounded text-sm disabled:bg-gray-400"
              >
                {isDownloading ? "Downloading..." : "📄 Download CSV"}
              </button>
              <button
                onClick={() => downloadSyncReport('json')}
                disabled={isDownloading}
                className="bg-blue-600 text-white px-3 py-1 rounded text-sm disabled:bg-gray-400"
              >
                📋 Download JSON
              </button>
            </>
          )}
        </div>
      </div>
      {syncMessage && <p className="text-sm text-green-700 mb-2">{syncMessage}</p>}
      
      <table className="table-auto w-full border">
        <thead>
          <tr className="bg-gray-200">
            <th className="border px-2 py-1">Source Table</th>
            <th className="border px-2 py-1">Target Table</th>
            <th className="border px-2 py-1">Matched</th>
            <th className="border px-2 py-1">Unmatched</th>
            <th className="border px-2 py-1">Missing</th>
            <th className="border px-2 py-1">Extra</th>
            <th className="border px-2 py-1">Status</th>
            <th className="border px-2 py-1">Actions</th>
          </tr>
        </thead>
        <tbody>
          {report.map((row, idx) => (
            <tr key={idx}>
              <td className="border px-2 py-1">{row.source_table}</td>
              <td className="border px-2 py-1">{row.target_table}</td>
              <td className="border px-2 py-1 text-green-600">{row.matched}</td>
              <td className="border px-2 py-1 text-red-600">{row.unmatched}</td>
              <td className="border px-2 py-1 text-orange-600">{row.missing_in_target || 0}</td>
              <td className="border px-2 py-1 text-purple-600">{row.extra_in_target || 0}</td>
              <td className="border px-2 py-1">
                <span className={`px-2 py-1 rounded text-xs ${
                  row.sync_status === 'Synced' ? 'bg-green-100 text-green-800' : 'bg-yellow-100 text-yellow-800'
                }`}>
                  {row.sync_status}
                </span>
              </td>
              <td className="border px-2 py-1 text-center">
                <div className="flex gap-2 justify-center">
                  {row.unmatched > 0 && (
                    <button
                      onClick={() => fetchDetailedData(row.source_table)}
                      className="bg-gray-600 text-white px-2 py-1 rounded text-sm"
                      title="View Details"
                    >
                      👁️
                    </button>
                  )}
                  {selectedRecords.size > 0 && (
                    <button
                      onClick={() => handleSync(row.source_table)}
                      className="bg-blue-600 text-white px-3 py-1 rounded text-sm"
                    >
                      Sync Selected ({selectedRecords.size})
                    </button>
                  )}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Detailed View Modal */}
      {showDetails && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 max-w-6xl max-h-[80vh] overflow-auto">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-bold">Detailed Comparison - {showDetails}</h3>
              <button
                onClick={() => setShowDetails(null)}
                className="text-gray-500 hover:text-gray-700 text-xl"
              >
                ×
              </button>
            </div>
            
            <div className="mb-4 bg-blue-50 p-3 rounded">
              <div className="flex justify-between items-center">
                <p className="text-sm font-medium text-blue-800">
                  📊 Selection Status: {selectedRecords.size} of {detailedData.length} records selected
                </p>
                <div className="flex gap-2">
                  <button
                    onClick={selectAllRecords}
                    className="bg-blue-600 text-white px-3 py-1 rounded text-xs hover:bg-blue-700"
                  >
                    ✅ Select All ({detailedData.length})
                  </button>
                  <button
                    onClick={clearAllSelection}
                    className="bg-gray-500 text-white px-3 py-1 rounded text-xs hover:bg-gray-600"
                  >
                    ❌ Clear All
                  </button>
                </div>
              </div>
              {selectedRecords.size > 0 && (
                <div className="mt-2 text-xs text-blue-600">
                  Ready to sync: {Array.from(selectedRecords).join(', ')}
                </div>
              )}
            </div>

            <div className="overflow-x-auto">
              <table className="table-auto w-full border text-sm">
                <thead>
                  <tr className="bg-gray-100">
                    <th className="border px-2 py-1">Select</th>
                    <th className="border px-2 py-1">Record ID</th>
                    <th className="border px-2 py-1">Type</th>
                    <th className="border px-2 py-1">Source Hash</th>
                    <th className="border px-2 py-1">Target Hash</th>
                    <th className="border px-2 py-1">Differences</th>
                    <th className="border px-2 py-1">Source Data</th>
                    <th className="border px-2 py-1">Target Data</th>
                  </tr>
                </thead>
                <tbody>
                  {detailedData.map((record, idx) => (
                    <tr key={idx} className={`${
                      record.difference_type === 'missing' ? 'bg-orange-50' :
                      record.difference_type === 'extra' ? 'bg-purple-50' :
                      'bg-red-50'
                    }`}>
                      <td className="border px-2 py-1 text-center">
                        <input
                          type="checkbox"
                          checked={selectedRecords.has(record.record_id)}
                          onChange={() => toggleRecordSelection(record.record_id)}
                          className="form-checkbox"
                        />
                      </td>
                      <td className="border px-2 py-1">{record.record_id}</td>
                      <td className="border px-2 py-1">
                        <span className={`px-2 py-1 rounded text-xs ${
                          record.difference_type === 'missing' ? 'bg-orange-200 text-orange-800' :
                          record.difference_type === 'extra' ? 'bg-purple-200 text-purple-800' :
                          'bg-red-200 text-red-800'
                        }`}>
                          {record.difference_type}
                        </span>
                      </td>
                      <td className="border px-2 py-1 font-mono text-xs">
                        {record.source_hash ? record.source_hash.substring(0, 8) + '...' : 'N/A'}
                      </td>
                      <td className="border px-2 py-1 font-mono text-xs">
                        {record.target_hash ? record.target_hash.substring(0, 8) + '...' : 'N/A'}
                      </td>
                      <td className="border px-2 py-1">
                        <ul className="text-xs">
                          {record.differences.map((diff, i) => (
                            <li key={i} className="mb-1">{diff}</li>
                          ))}
                        </ul>
                      </td>
                      <td className="border px-2 py-1">
                        <pre className="text-xs bg-gray-100 p-1 rounded">
                          {record.source_data ? JSON.stringify(record.source_data, null, 1) : 'N/A'}
                        </pre>
                      </td>
                      <td className="border px-2 py-1">
                        <pre className="text-xs bg-gray-100 p-1 rounded">
                          {record.target_data ? JSON.stringify(record.target_data, null, 1) : 'N/A'}
                        </pre>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="mt-4 flex justify-between items-center bg-gray-50 p-3 rounded">
              <div className="text-sm text-gray-600">
                💡 <strong>Tip:</strong> Use checkboxes above to select records, then click "Sync Selected" to apply SCD Type 3 changes
              </div>
              <div className="flex gap-2">
                {selectedRecords.size > 0 && (
                  <button
                    onClick={() => handleSync(showDetails!)}
                    className="bg-green-600 text-white px-6 py-2 rounded font-medium hover:bg-green-700 flex items-center gap-2"
                  >
                    🚀 Sync Selected ({selectedRecords.size})
                  </button>
                )}
                <button
                  onClick={() => setShowDetails(null)}
                  className="bg-gray-600 text-white px-4 py-2 rounded hover:bg-gray-700"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default ReportTable;
