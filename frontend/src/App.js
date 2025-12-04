import React, { useState, useEffect } from 'react';
import './App.css';

function App() {
  const [data, setData] = useState([]);

  useEffect(() => {
    const apiURL =
      window.location.hostname === "localhost" ||
      window.location.hostname.startsWith("192.")
        ? "http://192.168.18.94:5000/data"   // AKSES DARI LAPTOP
        : "http://api:5000/data";            // AKSES DARI DALAM DOCKER

    const fetchData = async () => {
      try {
        const response = await fetch(apiURL);
        const result = await response.json();
        setData(result);
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 2000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="App">
      <h1>Data Streaming</h1>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Timestamp</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          {data.map(item => (
            <tr key={item.id}>
              <td>{item.id}</td>
              <td>{item.timestamp}</td>
              <td>{item.value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
