import ConfigForm from "../components/ConfigForm";

export default function ConfigPage() {
  return (
    <div className="p-6">
      <h1 className="text-xl font-bold">Database Config</h1>
      <ConfigForm />
    </div>
  );
}
