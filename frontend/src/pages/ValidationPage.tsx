import { useState } from "react";
import api from "../api/api";
import { ENDPOINTS } from "../api/endpoints";
import ReportTable from "../components/ReportTable";

export default function ValidationPage() {
  const [tables, setTables] = useState("customers,orders");
  const [region, setRegion] = useState("");
  const [report, setReport] = useState<any>(null);

  const runValidation = async () => {
    const res = await api.post(ENDPOINTS.VALIDATE, { tables: tables.split(","), region });
    setReport(res.data.report);
  };

  return (
    <div className="p-6">
      <h1 className="text-xl font-bold">Validation</h1>
      <input
        value={tables}
        onChange={(e) => setTables(e.target.value)}
        className="border p-2 w-full"
      />
      <input
        value={region}
        onChange={(e) => setRegion(e.target.value)}
        placeholder="Region (optional)"
        className="border p-2 w-full mt-2"
      />
      <button
        onClick={runValidation}
        className="bg-green-600 text-white px-4 py-2 rounded mt-2"
      >
        Run Validation
      </button>

      {report && <ReportTable report={report} />}
    </div>
  );
}
