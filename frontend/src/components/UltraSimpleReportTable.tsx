import { useState, useEffect, useCallback } from "react";

interface UltraSimpleReportTableProps {
  refreshTrigger: number;
  selectedTables: string[];
  scdType2Enabled?: boolean;
}

interface ComparisonResult {
  table: string;
  source_count: number;
  target_count: number;
  matched_count: number;
  unmatched_count: number;
  missing_in_target: number;
  extra_in_target: number;
  changed_count?: number;
  status: string;
}

interface DetailedRecord {
  record_id: any;
  source_data?: any;
  target_data?: any;
  difference_type: string;
  differences: string[];
}

function UltraSimpleReportTable({ refreshTrigger, selectedTables, scdType2Enabled = false }: UltraSimpleReportTableProps) {
  const [report, setReport] = useState<ComparisonResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [showDetails, setShowDetails] = useState<string | null>(null);
  const [detailedData, setDetailedData] = useState<DetailedRecord[]>([]);
  const [csvDownloadUrl, setCsvDownloadUrl] = useState<string>("");
  const [excelDownloading, setExcelDownloading] = useState<boolean>(false);

  const generateCSVData = (results: ComparisonResult[]): string => {
    const headers = ['Table', 'Source Count', 'Target Count', 'Matched', 'Changed', 'Missing', 'Extra', 'Status'];
    const csvContent = [
      headers.join(','),
      ...results.map(result => [
        result.table,
        result.source_count,
        result.target_count,
        result.matched_count,
        result.changed_count || 0,
        result.missing_in_target,
        result.extra_in_target,
        result.status
      ].join(','))
    ].join('\n');
    return csvContent;
  };

  const fetchReport = useCallback(async () => {
    if (selectedTables.length === 0) {
      setReport([]);
      return;
    }

    setLoading(true);
    setMessage("🔄 Comparing tables...");
    
    try {
      const response = await fetch("http://localhost:8000/pyspark/compare-multiple-tables", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ 
          tables: selectedTables,
          scd_type: scdType2Enabled ? "scd2" : "scd3"
        }),
      });
      
      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }
      
      const data = await response.json();
      setReport(data.results || []);
      setMessage("✅ Comparison completed!");
      
      // Generate CSV download URL
      const csvData = generateCSVData(data.results || []);
      const blob = new Blob([csvData], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      setCsvDownloadUrl(url);
      
      // Clear message after 3 seconds
      setTimeout(() => setMessage(""), 3000);
      
    } catch (error) {
      console.error("Error:", error);
      setMessage(`❌ Error: ${error}`);
    } finally {
      setLoading(false);
    }
  }, [selectedTables, scdType2Enabled]);

  const viewDetails = useCallback(async (tableName: string) => {
    setMessage("🔍 Loading details...");
    
    try {
      const response = await fetch("http://localhost:8000/pyspark/detailed-comparison", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          table: tableName,
          region: "ALL"
        }),
      });
      
      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }
      
      const data = await response.json();
      setDetailedData(data.detailed_records || []);
      setShowDetails(tableName);
      setMessage("✅ Details loaded successfully!");
      
      // Clear message after 3 seconds
      setTimeout(() => setMessage(""), 3000);
      
    } catch (error) {
      console.error("Error loading details:", error);
      setMessage(`❌ Error loading details: ${error}`);
    }
  }, []);

  const closeDetails = () => {
    setShowDetails(null);
    setDetailedData([]);
  };

  const syncRecords = async (tableName: string) => {
    setMessage("🔄 Syncing records...");
    
    try {
      const response = await fetch("http://localhost:8000/pyspark/sync-records", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          table: tableName,
          scd_type: scdType2Enabled ? "scd2" : "scd3"
        }),
      });
      
      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }
      
      const data = await response.json();
      setMessage(`✅ Sync completed! Synced ${data.synced_count}/${data.total_source_records} records`);
      
      // Refresh the report after sync
      setTimeout(() => {
        fetchReport();
        setMessage("");
      }, 3000);
      
    } catch (error) {
      console.error("Error syncing records:", error);
      setMessage(`❌ Error syncing records: ${error}`);
    }
  };

  const checkSCDType2 = async (tableName: string) => {
    setMessage("🔍 Checking SCD Type 2 implementation...");

    try {
      const response = await fetch("http://localhost:8000/azure-compare/check-scd-type2");

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      const data = await response.json();

      let scdInfo = `📊 SCD Type 2 Check for ${data.table}:\n\n`;
      scdInfo += `SCD Type 2 Columns:\n`;
      scdInfo += `• Effective Date: ${data.scd_type2_columns.effective_date ? '✅' : '❌'}\n`;
      scdInfo += `• Expiry Date: ${data.scd_type2_columns.expiry_date ? '✅' : '❌'}\n`;
      scdInfo += `• Is Current: ${data.scd_type2_columns.is_current ? '✅' : '❌'}\n`;
      scdInfo += `• Version: ${data.scd_type2_columns.version ? '✅' : '❌'}\n\n`;
      scdInfo += `Audit Columns:\n`;
      scdInfo += `• Created Date: ${data.audit_columns.created_date ? '✅' : '❌'}\n`;
      scdInfo += `• Updated Date: ${data.audit_columns.updated_date ? '✅' : '❌'}\n\n`;
      scdInfo += `Has SCD Type 2: ${data.has_scd_type2 ? '✅ Yes' : '❌ No'}\n\n`;
      scdInfo += `Recommendation: ${data.recommendation}`;

      setMessage(scdInfo);

      // Clear message after 10 seconds
      setTimeout(() => setMessage(""), 10000);

    } catch (error) {
      console.error("Error checking SCD Type 2:", error);
      setMessage(`❌ Error checking SCD Type 2: ${error}`);
    }
  };

  const downloadExcelReport = async (tableName: string) => {
    setExcelDownloading(true);
    setMessage("📊 Generating Excel report...");

    try {
      const response = await fetch("http://localhost:8000/pyspark/download-excel-report", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          table: tableName
        }),
      });

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      // Get the filename from the response headers
      const contentDisposition = response.headers.get('content-disposition');
      const filename = contentDisposition 
        ? contentDisposition.split('filename=')[1]?.replace(/"/g, '')
        : `validation_report_${tableName}_${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.xlsx`;

      // Create blob and download
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      setMessage("✅ Excel report downloaded successfully!");
      setTimeout(() => setMessage(""), 3000);

    } catch (error) {
      console.error("Error downloading Excel report:", error);
      setMessage(`❌ Error downloading Excel report: ${error}`);
    } finally {
      setExcelDownloading(false);
    }
  };

  useEffect(() => {
    fetchReport();
  }, [refreshTrigger, selectedTables]);

  return (
    <div className="mt-6">
      <div className="flex justify-between items-center mb-4">
        <div>
          <h2 className="text-xl font-bold text-gray-800">📊 Validation Results</h2>
          <div className="mt-2 text-sm text-gray-600">
            Simple Sync - No SCD complexity needed
          </div>
        </div>
        <div className="flex gap-2">
          {csvDownloadUrl && (
            <a
              href={csvDownloadUrl}
              download="validation_results.csv"
              className="bg-green-500 text-white px-4 py-2 rounded hover:bg-green-600 flex items-center gap-2"
            >
              <span>📊</span>
              Download CSV
            </a>
          )}
          {report.length > 0 && (
            <button
              onClick={() => downloadExcelReport(report[0].table)}
              disabled={excelDownloading}
              className="bg-purple-500 text-white px-4 py-2 rounded hover:bg-purple-600 disabled:bg-gray-400 flex items-center gap-2"
            >
              <span>📈</span>
              {excelDownloading ? "Generating..." : "Download Excel"}
            </button>
          )}
        <button
          onClick={fetchReport}
          disabled={loading}
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600 disabled:bg-gray-400"
        >
          {loading ? "🔄 Loading..." : "🔄 Refresh"}
        </button>
        </div>
      </div>

      {/* Status Message */}
      {message && (
        <div className={`p-3 rounded mb-4 text-center font-medium ${
          message.includes('✅') ? 'bg-green-100 text-green-800' : 
          message.includes('❌') ? 'bg-red-100 text-red-800' : 
          'bg-blue-100 text-blue-800'
        }`}>
          {message}
        </div>
      )}

      {/* SCD Type 2 Info */}
      {scdType2Enabled && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
          <div className="flex items-center gap-2">
            <span className="text-blue-600">ℹ️</span>
            <span className="text-blue-800 font-medium">SCD Type 2 Mode Active</span>
          </div>
          <p className="text-blue-700 text-sm mt-1">
            Historical tracking enabled - changes will create new records with effective dates
          </p>
        </div>
      )}

      {/* Results Table */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">TABLE</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">SOURCE</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">TARGET</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">✔ MATCHED</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">CHANGED</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">+ MISSING</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">+ EXTRA</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ACTIONS</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {report.map((result) => (
                <tr key={result.table} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {result.table}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {result.source_count}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {result.target_count}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-green-600 font-medium">
                    {result.matched_count}
                    </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-red-600 font-medium">
                    {result.changed_count || 0}
                    </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-red-600 font-medium">
                    {result.missing_in_target}
                    </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-red-600 font-medium">
                    {result.extra_in_target}
                    </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <div className="flex gap-2">
                      <button
                        onClick={() => viewDetails(result.table)}
                        className="text-blue-600 hover:text-blue-900 flex items-center gap-1"
                      >
                        <span>👁️</span>
                        View Details
                      </button>
                      <button
                        onClick={() => syncRecords(result.table)}
                        className="text-green-600 hover:text-green-900 flex items-center gap-1"
                      >
                        <span>🔄</span>
                        Sync
                      </button>
                      <button
                        onClick={() => checkSCDType2(result.table)}
                        className="text-purple-600 hover:text-purple-900 flex items-center gap-1"
                      >
                        <span>📊</span>
                        Check SCD
                      </button>
                    </div>
                  </td>
                  </tr>
              ))}
            </tbody>
          </table>
        </div>
        </div>

      {/* Detailed Comparison Modal */}
      {showDetails && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-6xl w-full mx-4 max-h-[90vh] overflow-hidden">
            <div className="flex justify-between items-center p-6 border-b">
              <h3 className="text-lg font-semibold text-gray-900">
                Detailed Comparison: {showDetails}
              </h3>
              <button
                onClick={closeDetails}
                className="text-gray-400 hover:text-gray-600 text-2xl"
              >
                ×
              </button>
            </div>
            <div className="p-6 overflow-y-auto max-h-[calc(90vh-120px)]">
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Record ID</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Source Data</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Target Data</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Differences</th>
                  </tr>
                </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                  {detailedData.map((record, index) => (
                    <tr key={index} className="hover:bg-gray-50">
                        <td className="px-3 py-2 text-sm font-medium text-gray-900">
                          {record.record_id}
                      </td>
                        <td className="px-3 py-2 text-sm">
                          <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                          record.difference_type === 'matched' ? 'bg-green-100 text-green-800' :
                            record.difference_type === 'mismatch' ? 'bg-yellow-100 text-yellow-800' :
                            record.difference_type === 'missing' ? 'bg-red-100 text-red-800' :
                            'bg-blue-100 text-blue-800'
                        }`}>
                          {record.difference_type}
                        </span>
                      </td>
                      <td className="px-3 py-2 max-w-md">
                        <div className="whitespace-pre-wrap break-words text-xs font-mono bg-gray-50 p-2 rounded border">
                          {record.source_data ? JSON.stringify(record.source_data, null, 2) : 'N/A'}
                        </div>
                      </td>
                      <td className="px-3 py-2 max-w-md">
                        <div className="whitespace-pre-wrap break-words text-xs font-mono bg-gray-50 p-2 rounded border">
                          {record.target_data ? JSON.stringify(record.target_data, null, 2) : 'N/A'}
                        </div>
                      </td>
                      <td className="px-3 py-2 max-w-md">
                        <div className="whitespace-pre-wrap break-words text-xs font-mono bg-gray-50 p-2 rounded border">
                          {record.differences?.join(', ') || 'None'}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default UltraSimpleReportTable;