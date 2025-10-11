import React, { useState, useEffect } from 'react';

interface ConnectionData {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  [key: string]: any; // For additional fields
}

interface CloudDetectionResponse {
  cloud_type: string;
  additional_fields: Array<{
    name: string;
    label: string;
    type: string;
    placeholder?: string;
    required?: boolean;
    help?: string;
    options?: string[];
    default?: any;
  }>;
  connection_string_template: string;
  detected: boolean;
}

interface SmartConnectionFormProps {
  title: string;
  connectionData: ConnectionData;
  onConnectionDataChange: (data: ConnectionData) => void;
  onTestConnection: () => void;
  connectionStatus: 'idle' | 'testing' | 'success' | 'error';
  connectionMessage: string;
  color: 'blue' | 'green';
}

const SmartConnectionForm: React.FC<SmartConnectionFormProps> = ({
  title,
  connectionData,
  onConnectionDataChange,
  onTestConnection,
  connectionStatus,
  connectionMessage,
  color
}) => {
  const [cloudDetection, setCloudDetection] = useState<CloudDetectionResponse | null>(null);
  const [detecting, setDetecting] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);

  // Auto-detect cloud type when host/port/database changes
  useEffect(() => {
    const detectCloudType = async () => {
      if (!connectionData.host || !connectionData.port || !connectionData.database) {
        setCloudDetection(null);
        return;
      }

      setDetecting(true);
      try {
        const response = await fetch('http://localhost:8000/cloud-detection/detect', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            host: connectionData.host,
            port: connectionData.port,
            database: connectionData.database
          })
        });

        if (response.ok) {
          const detection = await response.json();
          setCloudDetection(detection);
          
          // Auto-fill default values for additional fields
          if (detection.additional_fields) {
            const newData = { ...connectionData };
            detection.additional_fields.forEach((field: any) => {
              if (field.default !== undefined && !newData[field.name]) {
                newData[field.name] = field.default;
              }
            });
            onConnectionDataChange(newData);
          }
        }
      } catch (error) {
        console.error('Cloud detection failed:', error);
      } finally {
        setDetecting(false);
      }
    };

    // Debounce detection
    const timeoutId = setTimeout(detectCloudType, 500);
    return () => clearTimeout(timeoutId);
  }, [connectionData.host, connectionData.port, connectionData.database]);

  const handleInputChange = (field: string, value: any) => {
    onConnectionDataChange({
      ...connectionData,
      [field]: value
    });
  };

  const renderField = (field: any) => {
    const value = connectionData[field.name] || field.default || '';

    switch (field.type) {
      case 'select':
        return (
          <select
            value={value}
            onChange={(e) => handleInputChange(field.name, e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            required={field.required}
          >
            {field.options?.map((option: string) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        );

      case 'checkbox':
        return (
          <div className="flex items-center">
            <input
              type="checkbox"
              checked={value}
              onChange={(e) => handleInputChange(field.name, e.target.checked)}
              className="mr-2"
            />
            <span className="text-sm text-gray-600">{field.label}</span>
          </div>
        );

      default:
        return (
          <input
            type={field.type === 'password' ? 'password' : 'text'}
            value={value}
            onChange={(e) => handleInputChange(field.name, e.target.value)}
            placeholder={field.placeholder}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            required={field.required}
          />
        );
    }
  };

  const getStatusColor = () => {
    switch (connectionStatus) {
      case 'success': return 'bg-green-100 border-green-500 text-green-700';
      case 'error': return 'bg-red-100 border-red-500 text-red-700';
      case 'testing': return 'bg-yellow-100 border-yellow-500 text-yellow-700';
      default: return 'bg-gray-100 border-gray-300 text-gray-700';
    }
  };

  const getButtonColor = () => {
    return color === 'blue' 
      ? 'bg-blue-600 hover:bg-blue-700 focus:ring-blue-500'
      : 'bg-green-600 hover:bg-green-700 focus:ring-green-500';
  };

  return (
    <div className={`p-6 border-2 rounded-lg ${color === 'blue' ? 'border-blue-200' : 'border-green-200'}`}>
      <div className="flex items-center justify-between mb-4">
        <h3 className={`text-lg font-semibold ${color === 'blue' ? 'text-blue-800' : 'text-green-800'}`}>
          {title}
        </h3>
        {cloudDetection && (
          <div className="flex items-center space-x-2">
            <span className="text-sm text-gray-600">Detected:</span>
            <span className={`px-2 py-1 rounded text-xs font-medium ${
              color === 'blue' ? 'bg-blue-100 text-blue-800' : 'bg-green-100 text-green-800'
            }`}>
              {cloudDetection.cloud_type.replace('_', ' ').toUpperCase()}
            </span>
          </div>
        )}
      </div>

      {/* Basic Connection Fields */}
      <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Host
          </label>
          <input
            type="text"
            value={connectionData.host}
            onChange={(e) => handleInputChange('host', e.target.value)}
            placeholder="localhost or cloud endpoint"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Port
          </label>
          <input
            type="number"
            value={connectionData.port}
            onChange={(e) => handleInputChange('port', parseInt(e.target.value) || 0)}
            placeholder="3306, 5432, 1521, 1433"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Database
          </label>
          <input
            type="text"
            value={connectionData.database}
            onChange={(e) => handleInputChange('database', e.target.value)}
            placeholder="database name"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            User
          </label>
          <input
            type="text"
            value={connectionData.user}
            onChange={(e) => handleInputChange('user', e.target.value)}
            placeholder="username"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Password
          </label>
          <input
            type="password"
            value={connectionData.password}
            onChange={(e) => handleInputChange('password', e.target.value)}
            placeholder="password"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>

      {/* Cloud Detection Status */}
      {detecting && (
        <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-md">
          <div className="flex items-center">
            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600 mr-2"></div>
            <span className="text-sm text-blue-700">Detecting cloud database type...</span>
          </div>
        </div>
      )}

      {/* Additional Cloud Fields */}
      {cloudDetection && cloudDetection.additional_fields && cloudDetection.additional_fields.length > 0 && (
        <div className="mt-6">
          <button
            type="button"
            onClick={() => setShowAdvanced(!showAdvanced)}
            className="flex items-center text-sm font-medium text-gray-700 hover:text-gray-900"
          >
            <span className="mr-2">
              {showAdvanced ? '▼' : '▶'}
            </span>
            Advanced Cloud Settings
            <span className="ml-2 px-2 py-1 bg-gray-100 text-gray-600 rounded text-xs">
              {cloudDetection.additional_fields.length} fields
            </span>
          </button>

          {showAdvanced && (
            <div className="mt-4 space-y-4 p-4 bg-gray-50 rounded-md">
              {cloudDetection.additional_fields.map((field, index) => (
                <div key={index}>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    {field.label}
                    {field.required && <span className="text-red-500 ml-1">*</span>}
                  </label>
                  {renderField(field)}
                  {field.help && (
                    <p className="mt-1 text-xs text-gray-500">{field.help}</p>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Test Connection Button */}
      <div className="mt-6">
        <button
          onClick={onTestConnection}
          disabled={connectionStatus === 'testing'}
          className={`w-full px-4 py-2 text-white font-medium rounded-md focus:outline-none focus:ring-2 focus:ring-offset-2 ${getButtonColor()} disabled:opacity-50 disabled:cursor-not-allowed`}
        >
          {connectionStatus === 'testing' ? (
            <div className="flex items-center justify-center">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
              Testing Connection...
            </div>
          ) : (
            'Test Connection'
          )}
        </button>
      </div>

      {/* Connection Status */}
      {connectionMessage && (
        <div className={`mt-4 p-3 rounded-md border ${getStatusColor()}`}>
          <div className="flex items-center">
            {connectionStatus === 'success' && <span className="text-green-600 mr-2">✓</span>}
            {connectionStatus === 'error' && <span className="text-red-600 mr-2">✗</span>}
            {connectionStatus === 'testing' && <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-yellow-600 mr-2"></div>}
            <span className="text-sm">{connectionMessage}</span>
          </div>
        </div>
      )}
    </div>
  );
};

export default SmartConnectionForm;
