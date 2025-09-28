import React, { useState } from "react";

function ConfigForm() {
  const [sourceConfig, setSourceConfig] = useState({
    host: "localhost",
    port: 3306,
    database: "source_db",
    user: "root",
    password: "Sandy@123",
  });
  
  const [targetConfig, setTargetConfig] = useState({
    host: "localhost",
    port: 3306,
    database: "target_db",
    user: "root",
    password: "Sandy@123",
  });
  
  const [sourceMessage, setSourceMessage] = useState("");
  const [targetMessage, setTargetMessage] = useState("");
  const [sourceConnected, setSourceConnected] = useState(false);
  const [targetConnected, setTargetConnected] = useState(false);

  const handleSourceChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSourceConfig({ ...sourceConfig, [e.target.name]: e.target.value });
  };

  const handleTargetChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setTargetConfig({ ...targetConfig, [e.target.name]: e.target.value });
  };

  const testSourceConnection = async () => {
    try {
      const response = await fetch("http://localhost:8000/db/test-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(sourceConfig),
      });
      const data = await response.json();
      setSourceMessage(data.message);
      setSourceConnected(response.ok);
    } catch (err) {
      setSourceMessage("Error connecting to source database");
      setSourceConnected(false);
    }
  };

  const testTargetConnection = async () => {
    try {
      const response = await fetch("http://localhost:8000/db/test-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(targetConfig),
      });
      const data = await response.json();
      setTargetMessage(data.message);
      setTargetConnected(response.ok);
    } catch (err) {
      setTargetMessage("Error connecting to target database");
      setTargetConnected(false);
    }
  };

  return (
    <div className="p-4 bg-white rounded shadow">
      <h2 className="text-lg font-bold mb-4">Database Configuration</h2>
      
      {/* Source Database */}
      <div className="mb-6">
        <h3 className="text-md font-semibold mb-2 flex items-center">
          Source Database 
          {sourceConnected && <span className="ml-2 bg-green-100 text-green-800 px-2 py-1 rounded text-xs">✓ Connected</span>}
          {!sourceConnected && <span className="ml-2 bg-red-100 text-red-800 px-2 py-1 rounded text-xs">✗ Not Connected</span>}
        </h3>
        <div className="grid grid-cols-2 gap-2">
          <input name="host" placeholder="Host" value={sourceConfig.host} onChange={handleSourceChange} className="border p-2" />
          <input name="port" placeholder="Port" type="number" value={sourceConfig.port} onChange={handleSourceChange} className="border p-2" />
          <input name="database" placeholder="Database" value={sourceConfig.database} onChange={handleSourceChange} className="border p-2" />
          <input name="user" placeholder="User" value={sourceConfig.user} onChange={handleSourceChange} className="border p-2" />
          <input name="password" placeholder="Password" type="password" value={sourceConfig.password} onChange={handleSourceChange} className="border p-2" />
        </div>
        <button onClick={testSourceConnection} className="mt-3 bg-blue-600 text-white px-4 py-2 rounded">
          Test Source Connection
        </button>
        {sourceMessage && <p className={`mt-2 text-sm ${sourceConnected ? 'text-green-700' : 'text-red-700'}`}>{sourceMessage}</p>}
      </div>

      {/* Target Database */}
      <div>
        <h3 className="text-md font-semibold mb-2 flex items-center">
          Target Database 
          {targetConnected && <span className="ml-2 bg-green-100 text-green-800 px-2 py-1 rounded text-xs">✓ Connected</span>}
          {!targetConnected && <span className="ml-2 bg-red-100 text-red-800 px-2 py-1 rounded text-xs">✗ Not Connected</span>}
        </h3>
        <div className="grid grid-cols-2 gap-2">
          <input name="host" placeholder="Host" value={targetConfig.host} onChange={handleTargetChange} className="border p-2" />
          <input name="port" placeholder="Port" type="number" value={targetConfig.port} onChange={handleTargetChange} className="border p-2" />
          <input name="database" placeholder="Database" value={targetConfig.database} onChange={handleTargetChange} className="border p-2" />
          <input name="user" placeholder="User" value={targetConfig.user} onChange={handleTargetChange} className="border p-2" />
          <input name="password" placeholder="Password" type="password" value={targetConfig.password} onChange={handleTargetChange} className="border p-2" />
        </div>
        <button onClick={testTargetConnection} className="mt-3 bg-green-600 text-white px-4 py-2 rounded">
          Test Target Connection
        </button>
        {targetMessage && <p className={`mt-2 text-sm ${targetConnected ? 'text-green-700' : 'text-red-700'}`}>{targetMessage}</p>}
      </div>
    </div>
  );
}

export default ConfigForm;
