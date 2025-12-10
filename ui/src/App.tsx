import { useEffect, useMemo, useState } from 'react';
import MapView from './components/MapView';
import ControlPanel from './components/ControlPanel';
import TrendChart from './components/TrendChart';
import useCrowdingStream from './hooks/useCrowdingStream';
import useStops from './hooks/useStops';
import useForecast from './hooks/useForecast';
import type { PredictionSelection } from './types';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

const App = () => {
  const { points } = useCrowdingStream(API_BASE);
  const { stops, loading: stopsLoading } = useStops(API_BASE);

  const [selection, setSelection] = useState<PredictionSelection>({
    origin_stop: 'place-dwnxg',
    dest_stop: 'place-pktrm',
    horizon_min: 10,
  });
  const [prediction, setPrediction] = useState<{
    p50: number;
    p90: number;
    origin_name?: string;
    dest_name?: string;
  } | null>(null);

  // Fetch TFT forecast data
  const { forecastData, loading: forecastLoading } = useForecast({
    apiBase: API_BASE,
    originStop: selection.origin_stop,
    destStop: selection.dest_stop,
    horizonMin: selection.horizon_min,
    enabled: true,
    refreshInterval: 30000,
  });

  const fetchPrediction = async (sel: PredictionSelection) => {
    try {
      const response = await fetch(`${API_BASE}/predict`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ requests: [sel] }),
      });
      if (response.ok) {
        const payload = await response.json();
        setPrediction({
          p50: payload[0].p50,
          p90: payload[0].p90,
          origin_name: payload[0].origin_name,
          dest_name: payload[0].dest_name,
        });
      }
    } catch (err) {
      console.error('Prediction error:', err);
    }
  };

  useEffect(() => {
    fetchPrediction(selection).catch((err) => console.error(err));
  }, [selection]);

  const recentPoints = useMemo(() => points.slice(-20), [points]);

  return (
    <div className="app-shell">
      <div className="map-pane">
        <MapView
          features={points}
          selection={selection}
          onSelect={(origin, dest) =>
            setSelection({ ...selection, origin_stop: origin, dest_stop: dest })
          }
        />
      </div>
      <div className="sidebar">
        <ControlPanel
          selection={selection}
          onChange={setSelection}
          prediction={prediction}
          forecastData={forecastData}
          forecastLoading={forecastLoading}
          stops={stops}
          stopsLoading={stopsLoading}
        />
        <TrendChart data={recentPoints} forecastData={forecastData} />
      </div>
    </div>
  );
};

export default App;
