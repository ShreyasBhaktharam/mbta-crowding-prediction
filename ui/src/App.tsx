import { useEffect, useMemo, useState } from 'react';
import MapView from './components/MapView';
import ControlPanel from './components/ControlPanel';
import TrendChart from './components/TrendChart';
import useCrowdingStream from './hooks/useCrowdingStream';
import type { PredictionSelection } from './types';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

const App = () => {
  const { points } = useCrowdingStream(API_BASE);
  const [selection, setSelection] = useState<PredictionSelection>({
    origin_stop: 'place-dwnxg',
    dest_stop: 'place-pktrm',
    horizon_min: 10,
  });
  const [prediction, setPrediction] = useState<{ p50: number; p90: number } | null>(null);

  const fetchPrediction = async (sel: PredictionSelection) => {
    const response = await fetch(`${API_BASE}/predict`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ requests: [sel] }),
    });
    if (response.ok) {
      const payload = await response.json();
      setPrediction({ p50: payload[0].p50, p90: payload[0].p90 });
    }
  };

  useEffect(() => {
    fetchPrediction(selection).catch((err) => console.error(err));
  }, [selection]);

  const recentPoints = useMemo(() => points.slice(-20), [points]);

  return (
    <div className="app-shell">
      <div className="map-pane">
        <MapView features={points} selection={selection} onSelect={(origin, dest) => setSelection({ ...selection, origin_stop: origin, dest_stop: dest })} />
      </div>
      <div className="sidebar">
        <ControlPanel selection={selection} onChange={setSelection} prediction={prediction} />
        <TrendChart data={recentPoints} />
      </div>
    </div>
  );
};

export default App;
