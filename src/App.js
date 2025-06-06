// App.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { COUNTRIES_LIST, COUNTRIES } from './COUNTRIES';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "http://localhost:8000/api";

function App() {
  const [inputType, setInputType] = useState('city');
  const [selectedCountry, setSelectedCountry] = useState('India');
  const [cityInput, setCityInput] = useState('');
  const [zipInput, setZipInput] = useState('');
  const [latLonInput, setLatLonInput] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [weather, setWeather] = useState(null);
  const [error, setError] = useState(null);
  const [fetchedLocationCoords, setFetchedLocationCoords] = useState(null);

  // Database Management States
  const [showDbManagement, setShowDbManagement] = useState(false);
  const [dbTableNames, setDbTableNames] = useState([]);
  const [selectedDbTable, setSelectedDbTable] = useState('weather_cache'); // Default to weather_cache
  const [dbTableColumns, setDbTableColumns] = useState([]);
  const [dbPkColumns, setDbPkColumns] = useState([]);
  const [editableColumns, setEditableColumns] = useState([]); // New state for editable columns
  const [selectedOrderColumn, setSelectedOrderColumn] = useState('');
  const [orderDirection, setOrderDirection] = useState('ASC');
  const [dbTableData, setDbTableData] = useState([]);
  const [selectedFieldToUpdate, setSelectedFieldToUpdate] = useState('');
  const [newFieldValue, setNewFieldValue] = useState('');
  const [pkValues, setPkValues] = useState({});

  // Weather fetching useEffect
  useEffect(() => {
    if (inputType === 'city' && cityInput.length > 2) {
      const timer = setTimeout(() => {
        axios.get(`${API_BASE_URL}/suggest-locations?query=${cityInput}`).then(res => setSuggestions(res.data)).catch(console.error);
      }, 300);
      return () => clearTimeout(timer);
    } else setSuggestions([]);
  }, [cityInput, inputType]);

  // DB Management useEffects
  useEffect(() => {
    if (showDbManagement) {
      fetchDbTableNames();
    }
  }, [showDbManagement]);

  useEffect(() => {
    if (selectedDbTable) {
      fetchTableMetadata(selectedDbTable);
      fetchDbData(selectedDbTable, selectedOrderColumn, orderDirection);
    }
  }, [selectedDbTable, selectedOrderColumn, orderDirection, showDbManagement]); // Re-fetch on table/sort change

  const fetchWeather = async () => {
    setError(null); setWeather(null); setFetchedLocationCoords(null);
    try {
      let payload;
      if (inputType === 'city') payload = { type: 'city', city: cityInput };
      else if (inputType === 'zip') payload = { type: 'zip', zip: zipInput, country: selectedCountry };
      else if (inputType === 'gps') {
        const [lat, lon] = latLonInput.split(',').map(Number);
        if (isNaN(lat) || isNaN(lon) || latLonInput.trim() === '') throw new Error("Invalid GPS: 'lat,lon' format needed.");
        payload = { type: 'gps', lat, lon };
      }
      const res = await axios.post(`${API_BASE_URL}/weather/current`, payload);
      setWeather(res.data);
      setFetchedLocationCoords({ name: res.data.name, lat: res.data.coord.lat, lon: res.data.coord.lon });
    } catch (err) { setError(err.response?.data?.detail || err.message || "Error occurred."); }
  };

  const getCurrentLocation = () => {
    setError(null);
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          const lat = position.coords.latitude.toFixed(2);
          const lon = position.coords.longitude.toFixed(2);
          setLatLonInput(`${lat}, ${lon}`);
        },
        (geoError) => {
          setError(`Geolocation error: ${geoError.message}`);
        }
      );
    } else {
      setError("Geolocation not supported by this browser.");
    }
  };

  // DB Management Functions
  const fetchDbTableNames = async () => {
    try {
      const res = await axios.get(`${API_BASE_URL}/db/tables`);
      setDbTableNames(res.data);
      if (!selectedDbTable && res.data.length > 0) {
        setSelectedDbTable(res.data[0]); // Auto-select first table if none chosen
      }
    } catch (err) { setError(err.response?.data?.detail || err.message || "Error fetching table names."); }
  };

  const fetchTableMetadata = async (tableName) => {
    try {
      const columnsRes = await axios.get(`${API_BASE_URL}/db/columns/${tableName}`);
      setDbTableColumns(columnsRes.data);

      const pkRes = await axios.get(`${API_BASE_URL}/db/pk_columns/${tableName}`);
      setDbPkColumns(pkRes.data);
      setPkValues(Object.fromEntries(pkRes.data.map(col => [String(col), '']))); // Ensure keys are strings for PK values

      // Fetch editable columns from the backend
      const editableRes = await axios.get(`${API_BASE_URL}/db/editable_columns/${tableName}`);
      setEditableColumns(editableRes.data);
      setSelectedFieldToUpdate(editableRes.data[0] || ''); // Default to first editable column

      setSelectedOrderColumn(pkRes.data[0] || columnsRes.data[0] || ''); // Default order by PK or first column
    } catch (err) { setError(err.response?.data?.detail || err.message || "Error fetching table metadata."); }
  };

  const fetchDbData = async (tableName, orderBy, orderDirection) => {
    try {
      const res = await axios.get(`${API_BASE_URL}/db/data/${tableName}`, {
        params: { order_by_col: orderBy, order_direction: orderDirection }
      });
      // Try to parse 'data' field if it's a JSON string for weather_cache
      const parsedData = res.data.map(row => {
        if (tableName === 'weather_cache' && row.data && typeof row.data === 'string') {
          try {
            return { ...row, data: JSON.parse(row.data) };
          } catch (e) {
            console.warn("Could not parse 'data' field:", row.data, e);
            return row; // Keep original if parsing fails
          }
        }
        return row;
      });
      setDbTableData(parsedData);
    } catch (err) { setError(err.response?.data?.detail || err.message || "Error fetching DB data."); }
  };

  const handleUpdateRecord = async () => {
    setError(null);
    try {
      if (!selectedDbTable || !selectedFieldToUpdate || dbPkColumns.some(pkCol => !pkValues[pkCol])) {
        throw new Error("Please select a table, a field, and provide all primary key values.");
      }

      // Explicitly check if the selected field is allowed to be updated
      if (!editableColumns.includes(selectedFieldToUpdate)) {
        throw new Error(`Column '${selectedFieldToUpdate}' is not editable for table '${selectedDbTable}'.`);
      }
      
      let valueToSend = newFieldValue;
      // Handle 'data' field specifically for weather_cache to ensure it's stringified JSON
      if (selectedDbTable === 'weather_cache' && selectedFieldToUpdate === 'data') {
        try {
          // If the user modified the JSON, parse it before sending
          valueToSend = JSON.parse(newFieldValue);
        } catch (e) {
          throw new Error("Invalid JSON for 'data' field.");
        }
      }

      // Convert PK values to correct types if needed (e.g., numbers for lat, lon, data_ts)
      const pk_dict_typed = {};
      for (const col of dbPkColumns) {
        if (col === 'lat' || col === 'lon' || col === 'data_ts' || col === 'fetch_ts') {
          pk_dict_typed[col] = Number(pkValues[col]);
          if (isNaN(pk_dict_typed[col])) {
              throw new Error(`Invalid number for primary key field: ${col}`);
          }
        } else {
          pk_dict_typed[col] = pkValues[col];
        }
      }


      await axios.put(`${API_BASE_URL}/db/record/${selectedDbTable}`, {
        pk_dict: pk_dict_typed,
        field_to_update: selectedFieldToUpdate,
        new_value: valueToSend
      });
      await fetchDbData(selectedDbTable, selectedOrderColumn, orderDirection); // Refresh data
      alert("Record updated successfully!");
    } catch (err) { setError(err.response?.data?.detail || err.message || "Error updating record."); }
  };

  const handleDeleteRecord = async () => {
    setError(null);
    try {
      if (!selectedDbTable || dbPkColumns.some(pkCol => !pkValues[pkCol])) {
        throw new Error("Please select a table and provide all primary key values for deletion.");
      }
      if (!window.confirm("Are you sure you want to delete this record? This action cannot be undone.")) {
        return;
      }

      // Convert PK values to correct types for deletion
      const pk_dict_typed = {};
      for (const col of dbPkColumns) {
        if (col === 'lat' || col === 'lon' || col === 'data_ts' || col === 'fetch_ts') {
          pk_dict_typed[col] = Number(pkValues[col]);
          if (isNaN(pk_dict_typed[col])) {
              throw new Error(`Invalid number for primary key field: ${col}`);
          }
        } else {
          pk_dict_typed[col] = pkValues[col];
        }
      }

      await axios.delete(`${API_BASE_URL}/db/record/${selectedDbTable}`, { data: { pk_dict: pk_dict_typed } });
      await fetchDbData(selectedDbTable, selectedOrderColumn, orderDirection); // Refresh data
      alert("Record deleted successfully!");
      // Clear PK inputs after successful deletion
      setPkValues(Object.fromEntries(dbPkColumns.map(col => [String(col), ''])));
    } catch (err) { setError(err.response?.data?.detail || err.message || "Error deleting record."); }
  };

  const handleRowClick = (rowData) => {
    const newPkValues = {};
    dbPkColumns.forEach(pkCol => {
      newPkValues[pkCol] = rowData[pkCol];
    });
    setPkValues(newPkValues);

    // Set selected field and value based on editable columns
    if (editableColumns.length > 0) {
        const defaultEditableCol = editableColumns[0];
        setSelectedFieldToUpdate(defaultEditableCol);
        // Special handling for 'data' field (JSON string)
        if (defaultEditableCol === 'data' && typeof rowData[defaultEditableCol] === 'object') {
            setNewFieldValue(JSON.stringify(rowData[defaultEditableCol], null, 2));
        } else {
            setNewFieldValue(rowData[defaultEditableCol]);
        }
    } else {
        setSelectedFieldToUpdate('');
        setNewFieldValue('');
    }
  };


  return (
    <div style={{ maxWidth: '800px', margin: 'auto', padding: '20px', color: '#fff', background: '#282c34' }}>
      <h1 style={{ textAlign: 'center' }}>Current Weather</h1>
      <div style={{ marginBottom: '20px' }}>
        <h3>Select input type:</h3>
        <label><input type="radio" value="city" checked={inputType === 'city'} onChange={() => setInputType('city')} />City Name</label>
        <label style={{ marginLeft: '20px' }}><input type="radio" value="zip" checked={inputType === 'zip'} onChange={() => setInputType('zip')} />Zip Code</label>
        <label style={{ marginLeft: '20px' }}><input type="radio" value="gps" checked={inputType === 'gps'} onChange={() => setInputType('gps')} />GPS Coordinates</label>
      </div>

      {inputType === 'zip' && (
        <div style={{ marginBottom: '15px' }}>
          <label>Select Country</label>
          <select value={selectedCountry} onChange={e => setSelectedCountry(e.target.value)} style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#3a3e47', color: '#fff', border: '1px solid #555' }}>
            {COUNTRIES_LIST.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      )}

      {inputType === 'city' && (
        <div style={{ marginBottom: '15px' }}>
          <label>Enter City Name</label>
          <input type="text" value={cityInput} onChange={e => setCityInput(e.target.value)} placeholder="Enter 3 letters for suggestions" style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#3a3e47', color: '#fff', border: '1px solid #555' }} />
          {suggestions.length > 0 && <div style={{ background: '#3a3e47', border: '1px solid #555', maxHeight: '150px', overflowY: 'auto' }}>
            {suggestions.map((s, i) => <div key={i} onClick={() => { setCityInput(s); setSuggestions([]); }} style={{ padding: '8px', cursor: 'pointer', '&:hover': { background: '#555' } }}>{s}</div>)}
          </div>}
        </div>
      )}
      {inputType === 'zip' && (<div style={{ marginBottom: '15px' }}><label>Enter Zip Code for {selectedCountry}</label><input type="text" value={zipInput} onChange={e => setZipInput(e.target.value)} placeholder="e.g., 10001" style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#3a3e47', color: '#fff', border: '1px solid #555' }} /></div>)}
      {inputType === 'gps' && (
        <div style={{ marginBottom: '15px' }}>
          <label>Enter coordinates (Lat, Lon)</label>
          <input type="text" value={latLonInput} onChange={e => setLatLonInput(e.target.value)} placeholder="e.g., 34.05, -118.24" style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#3a3e47', color: '#fff', border: '1px solid #555' }} />
          <button onClick={getCurrentLocation} style={{ padding: '8px 15px', background: '#28a745', color: 'white', border: 'none', cursor: 'pointer', borderRadius: '5px', marginTop: '10px' }}>Get Current Location</button>
        </div>
      )}
      <button onClick={fetchWeather} style={{ padding: '10px 20px', background: '#007bff', color: 'white', border: 'none', cursor: 'pointer', borderRadius: '5px', marginTop: '10px' }}>Get Weather</button>
      {error && <div style={{ color: 'red', marginTop: '20px' }}>Error: {error}</div>}
      {fetchedLocationCoords && (<div style={{ background: '#3c3', color: '#fff', padding: '10px', marginTop: '20px', borderRadius: '5px' }}>Coordinates for "{fetchedLocationCoords.name}": Lat={fetchedLocationCoords.lat}, Lon={fetchedLocationCoords.lon}</div>)}

      {weather && (
        <div style={{ marginTop: '30px' }}>
          <h2>Current Weather</h2>
          <div style={{ background: '#007bff', color: '#fff', padding: '10px', borderRadius: '5px', marginBottom: '15px' }}>Condition: {weather.current.weather[0].description}</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '10px', background: '#3a3e47', padding: '15px', borderRadius: '5px' }}>
            {weather.current.temp && <div style={{ background: '#f8f9fa', color: '#000', padding: '8px 12px', borderRadius: '3px' }}>Temperature: {weather.current.temp.toFixed(2)}°C</div>}
            {weather.current.feels_like && <div style={{ background: '#f8f9fa', color: '#000', padding: '8px 12px', borderRadius: '3px' }}>Feels Like: {weather.current.feels_like.toFixed(2)}°C</div>}
            {weather.current.pressure && <div style={{ background: '#f8f9fa', color: '#000', padding: '8px 12px', borderRadius: '3px' }}>Pressure: {weather.current.pressure} hPa</div>}
            {weather.current.humidity && <div style={{ background: '#f8f9fa', color: '#000', padding: '8px 12px', borderRadius: '3px' }}>Humidity: {weather.current.humidity}%</div>}
            {weather.current.visibility && <div style={{ background: '#f8f9fa', color: '#000', padding: '8px 12px', borderRadius: '3px' }}>Visibility: {weather.current.visibility} meters</div>}
            {weather.current.wind_speed && <div style={{ background: '#f8f9fa', color: '#000', padding: '8px 12px', borderRadius: '3px' }}>Wind Speed: {weather.current.wind_speed} m/s</div>}
          </div>
          <h2 style={{ marginTop: '30px' }}>5-Day Forecast</h2>
          <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: '15px' }}>
            <thead><tr style={{ background: '#4a4e57' }}><th style={{ padding: '8px', border: '1px solid #555', textAlign: 'left' }}>Date</th><th style={{ padding: '8px', border: '1px solid #555', textAlign: 'left' }}>Temp (Day)</th><th style={{ padding: '8px', border: '1px solid #555', textAlign: 'left' }}>Humidity</th><th style={{ padding: '8px', border: '1px solid #555', textAlign: 'left' }}>Description</th></tr></thead>
            <tbody>
              {weather.daily && weather.daily.slice(0, 5).map((d, i) => (<tr key={i} style={{ background: i % 2 === 0 ? '#3a3e47' : '#32363e' }}><td style={{ padding: '8px', border: '1px solid #555' }}>{new Date(d.dt * 1000).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}</td><td style={{ padding: '8px', border: '1px solid #555' }}>{d.temp.day.toFixed(2)}°C</td><td style={{ padding: '8px', border: '1px solid #555' }}>{d.humidity}%</td><td style={{ padding: '8px', border: '1px solid #555' }}>{d.weather[0].description}</td></tr>))}
            </tbody>
          </table>
        </div>
      )}
      <div style={{ marginTop: '40px', borderTop: '1px solid #555', paddingTop: '20px' }}>
        <h1 style={{ textAlign: 'center' }}>Database Management</h1>
        <div style={{ marginBottom: '15px' }}>
          <label>Browse and Edit Database Data</label>
          <button
            onClick={() => setShowDbManagement(!showDbManagement)}
            style={{ padding: '8px 15px', background: showDbManagement ? '#dc3545' : '#17a2b8', color: 'white', border: 'none', cursor: 'pointer', borderRadius: '5px', marginLeft: '10px' }}
          >
            {showDbManagement ? 'Hide Database' : 'Show Database'}
          </button>
        </div>
        {showDbManagement && (
          <div style={{ marginTop: '20px', background: '#3a3e47', padding: '15px', borderRadius: '5px' }}>
            {/* Table Selection */}
            <div style={{ marginBottom: '15px', display: 'flex', alignItems: 'flex-end', gap: '10px' }}>
              <div style={{ flexGrow: 1 }}>
                <label>Select a Table</label>
                <select value={selectedDbTable} onChange={e => setSelectedDbTable(e.target.value)} style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#4a4e57', color: '#fff', border: '1px solid #555' }}>
                  {dbTableNames.map(name => <option key={name} value={name}>{name}</option>)}
                </select>
              </div>
              <button
                onClick={() => fetchDbData(selectedDbTable, selectedOrderColumn, orderDirection)}
                style={{ padding: '8px 15px', background: '#6c757d', color: 'white', border: 'none', cursor: 'pointer', borderRadius: '5px', minWidth: 'fit-content' }}
              >
                Refresh Data
              </button>
            </div>

            {/* Order by Column */}
            <div style={{ marginBottom: '15px' }}>
              <label>Order by Column</label>
              <select value={selectedOrderColumn} onChange={e => setSelectedOrderColumn(e.target.value)} style={{ width: '50%', padding: '8px', marginTop: '5px', background: '#4a4e57', color: '#fff', border: '1px solid #555' }}>
                {dbTableColumns.map(col => <option key={col} value={col}>{col}</option>)}
              </select>
              <label style={{ marginLeft: '10px' }}><input type="radio" value="ASC" checked={orderDirection === 'ASC'} onChange={() => setOrderDirection('ASC')} /> ASC</label>
              <label style={{ marginLeft: '10px' }}><input type="radio" value="DESC" checked={orderDirection === 'DESC'} onChange={() => setOrderDirection('DESC')} /> DESC</label>
            </div>

            {/* Display Table Data */}
            <div style={{ maxHeight: '300px', overflowY: 'auto', marginBottom: '20px', border: '1px solid #555' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ background: '#4a4e57' }}>
                    {dbTableColumns.map(col => <th key={col} style={{ padding: '8px', border: '1px solid #555', textAlign: 'left' }}>{col}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {dbTableData.length > 0 ? (
                    dbTableData.map((row, rowIndex) => (
                      <tr key={rowIndex} onClick={() => handleRowClick(row)} style={{ background: rowIndex % 2 === 0 ? '#3a3e47' : '#32363e', cursor: 'pointer' }}>
                        {dbTableColumns.map(col => (
                          <td key={col} style={{ padding: '8px', border: '1px solid #555', wordBreak: 'break-all' }}>
                            {col === 'data' ? (typeof row[col] === 'object' ? JSON.stringify(row[col]) : row[col]) : row[col]}
                          </td>
                        ))}
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={dbTableColumns.length} style={{ textAlign: 'center', padding: '15px' }}>No data available.</td></tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Edit Record Section */}
            <h2 style={{ textAlign: 'center', marginBottom: '15px' }}>Edit Record</h2>
            <div style={{ background: '#ffc107', color: '#000', padding: '10px', borderRadius: '5px', marginBottom: '10px', fontSize: '0.9em' }}>
              Directly editing database records can lead to data corruption if not careful. Only specific fields are editable.
            </div>
            <div style={{ background: '#17a2b8', color: '#fff', padding: '10px', borderRadius: '5px', marginBottom: '15px', fontSize: '0.9em' }}>
              To edit or delete a record, you must provide the exact values for its primary key: {dbPkColumns.join(', ')}
            </div>

            <div style={{ marginBottom: '15px' }}>
              <label>Select Field to Update</label>
              <select value={selectedFieldToUpdate} onChange={e => setSelectedFieldToUpdate(e.target.value)} style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#4a4e57', color: '#fff', border: '1px solid #555' }}>
                {editableColumns.length > 0 ? (
                  editableColumns.map(col => <option key={col} value={col}>{col}</option>)
                ) : (
                  <option value="">No editable fields for this table</option>
                )}
              </select>
            </div>

            <div style={{ marginBottom: '15px' }}>
              <label>Enter new value for '{selectedFieldToUpdate}'</label>
              <textarea
                value={newFieldValue}
                onChange={e => setNewFieldValue(e.target.value)}
                placeholder="Enter new value. Use valid JSON for 'data' field."
                rows="5"
                style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#4a4e57', color: '#fff', border: '1px solid #555' }}
                disabled={!selectedFieldToUpdate}
              ></textarea>
            </div>

            {dbPkColumns.map(pkCol => (
              <div key={pkCol} style={{ marginBottom: '10px' }}>
                <label>Value for PK: '{pkCol}'</label>
                <input
                  type="text"
                  value={pkValues[pkCol] !== undefined ? pkValues[pkCol] : ''} // Handle undefined
                  onChange={e => setPkValues(prev => ({ ...prev, [String(pkCol)]: e.target.value }))}
                  placeholder={`Enter value for ${pkCol}`}
                  style={{ width: '100%', padding: '8px', marginTop: '5px', background: '#4a4e57', color: '#fff', border: '1px solid #555' }}
                />
              </div>
            ))}

            <div style={{ display: 'flex', gap: '10px', marginTop: '20px' }}>
              <button onClick={handleUpdateRecord} style={{ padding: '10px 20px', background: '#007bff', color: 'white', border: 'none', cursor: 'pointer', borderRadius: '5px' }} disabled={!selectedFieldToUpdate}>Update Record</button>
              <button onClick={handleDeleteRecord} style={{ padding: '10px 20px', background: '#dc3545', color: 'white', border: 'none', cursor: 'pointer', borderRadius: '5px' }}>Delete Record</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
export default App;