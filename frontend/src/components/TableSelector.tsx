import React, { useState, useEffect } from "react";

interface TableSelectorProps {
  onValidationComplete: () => void;
}

function TableSelector({ onValidationComplete }: TableSelectorProps) {
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [selectedRegion, setSelectedRegion] = useState("");
  const [availableTables, setAvailableTables] = useState<string[]>(["customers", "orders", "products", "users", "transactions"]);
  const [availableRegions, setAvailableRegions] = useState<string[]>(["US", "EU", "ASIA", "GLOBAL"]);
  const [message, setMessage] = useState("");

  // Load available tables from API
  const fetchTablesFromAPI = async (database: string) => {
    try {
      const config = {
        host: "localhost",
        port: 3306,
        database: database,
        user: "root",
        password: "Sandy@123"
      };
      
      const response = await fetch("http://localhost:8000/db/get-tables", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });
      
      if (response.ok) {
        const data = await response.json();
        return data.tables;
      }
    } catch (err) {
      console.error("Failed to fetch tables:", err);
    }
    return [];
  };

  // Load tables when component mounts
  useEffect(() => {
    const loadTables = async () => {
      const sourceTables = await fetchTablesFromAPI("source_db");
      const targetTables = await fetchTablesFromAPI("target_db");
      
      // Combine and deduplicate tables
      const allTables = [...new Set([...sourceTables, ...targetTables])];
      if (allTables.length > 0) {
        setAvailableTables(allTables);
      }
    };
    
    loadTables();
  }, []);

  const handleTableChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const selectedOptions = Array.from(e.target.selectedOptions, option => option.value);
    setSelectedTables(selectedOptions);
  };

  const handleValidate = async () => {
    if (selectedTables.length === 0) {
      setMessage("Please select at least one table");
      return;
    }

    try {
      const response = await fetch("http://localhost:8000/validate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ 
          tables: selectedTables, 
          region: selectedRegion || null 
        }),
      });
      const data = await response.json();
      setMessage("Validation complete");
      onValidationComplete(); // ✅ trigger report refresh
    } catch (err) {
      setMessage("Error during validation");
    }
  };

  return (
    <div className="p-4 bg-white rounded shadow">
      <h2 className="text-lg font-bold mb-4">Table Selection</h2>
      
      <div className="space-y-4">
        {/* Region Selection */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Region (Optional)
          </label>
          <select
            value={selectedRegion}
            onChange={(e) => setSelectedRegion(e.target.value)}
            className="border p-2 w-full rounded"
          >
            <option value="">All Regions</option>
            {availableRegions.map((region) => (
              <option key={region} value={region}>
                {region}
              </option>
            ))}
          </select>
        </div>

        {/* Table Selection */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Tables (Select multiple)
          </label>
          <select
            multiple
            value={selectedTables}
            onChange={handleTableChange}
            className="border p-2 w-full rounded h-32"
            size={5}
          >
            {availableTables.map((table) => (
              <option key={table} value={table}>
                {table}
              </option>
            ))}
          </select>
          <p className="text-xs text-gray-500 mt-1">
            Hold Ctrl/Cmd to select multiple tables
          </p>
        </div>

        {/* Selected Tables Display */}
        {selectedTables.length > 0 && (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Selected Tables:
            </label>
            <div className="flex flex-wrap gap-2">
              {selectedTables.map((table) => (
                <span
                  key={table}
                  className="bg-blue-100 text-blue-800 px-2 py-1 rounded text-sm"
                >
                  {table}
                  <button
                    onClick={() => setSelectedTables(selectedTables.filter(t => t !== table))}
                    className="ml-1 text-blue-600 hover:text-blue-800"
                  >
                    ×
                  </button>
                </span>
              ))}
            </div>
          </div>
        )}
      </div>

      <button
        onClick={handleValidate}
        disabled={selectedTables.length === 0}
        className={`mt-4 px-4 py-2 rounded text-white ${
          selectedTables.length === 0
            ? 'bg-gray-400 cursor-not-allowed'
            : 'bg-green-600 hover:bg-green-700'
        }`}
      >
        Validate Selected Tables
      </button>
      
      {message && (
        <div className="mt-3 p-3 bg-gray-100 rounded">
          <pre className="text-sm text-gray-700 whitespace-pre-wrap">{message}</pre>
        </div>
      )}
    </div>
  );
}

export default TableSelector;
