import { useState } from 'react';
import type { Stop } from '../hooks/useStops';
import type { PredictionSelection } from '../types';
import type { ForecastData } from '../hooks/useForecast';

interface Props {
  selection: PredictionSelection;
  prediction: { p50: number; p90: number; origin_name?: string; dest_name?: string } | null;
  forecastData?: ForecastData | null;
  forecastLoading?: boolean;
  onChange: (sel: PredictionSelection) => void;
  stops: Stop[];
  stopsLoading: boolean;
}

// Crowding level thresholds and labels
const getCrowdingLevel = (score: number): { label: string; color: string; emoji: string } => {
  if (score < 0.25) return { label: 'Low', color: '#10b981', emoji: '🟢' };
  if (score < 0.5) return { label: 'Moderate', color: '#f59e0b', emoji: '🟡' };
  if (score < 0.75) return { label: 'High', color: '#f97316', emoji: '🟠' };
  return { label: 'Very High', color: '#ef4444', emoji: '🔴' };
};

const ControlPanel = ({ selection, prediction, forecastData, forecastLoading, onChange, stops, stopsLoading }: Props) => {
  const [showHelp, setShowHelp] = useState(false);

  const updateField = (field: keyof PredictionSelection, value: string | number) => {
    onChange({ ...selection, [field]: value });
  };

  const swapStops = () => {
    onChange({
      ...selection,
      origin_stop: selection.dest_stop,
      dest_stop: selection.origin_stop,
    });
  };

  // Find display names for current selection
  const originName = prediction?.origin_name || 
    stops.find((s) => s.stop_id === selection.origin_stop)?.stop_name || 
    selection.origin_stop;
  const destName = prediction?.dest_name ||
    stops.find((s) => s.stop_id === selection.dest_stop)?.stop_name || 
    selection.dest_stop;

  // Crowding scores (normalized 0-1)
  const typicalCrowding = prediction?.p50 ?? 0;
  const peakCrowding = prediction?.p90 ?? 0;
  
  const typicalLevel = getCrowdingLevel(typicalCrowding);
  const peakLevel = getCrowdingLevel(peakCrowding);

  const horizonOptions = [
    { value: 5, label: '5 min' },
    { value: 10, label: '10 min' },
    { value: 15, label: '15 min' },
    { value: 20, label: '20 min' },
    { value: 30, label: '30 min' },
  ];

  return (
    <div className="panel control-panel">
      <div className="panel-header">
        <h2>🚇 Crowding Forecast</h2>
        <button 
          className="help-toggle" 
          onClick={() => setShowHelp(!showHelp)}
          title="What do these numbers mean?"
        >
          ?
        </button>
      </div>

      {showHelp && (
        <div className="help-box">
          <p><strong>Crowding Index</strong> predicts how crowded a route segment will be:</p>
          <ul className="crowding-legend-list">
            <li><span className="legend-dot" style={{background: '#10b981'}}></span> <strong>Low (0-0.25)</strong>: Plenty of seats</li>
            <li><span className="legend-dot" style={{background: '#f59e0b'}}></span> <strong>Moderate (0.25-0.5)</strong>: Some standing</li>
            <li><span className="legend-dot" style={{background: '#f97316'}}></span> <strong>High (0.5-0.75)</strong>: Crowded</li>
            <li><span className="legend-dot" style={{background: '#ef4444'}}></span> <strong>Very High (0.75+)</strong>: Packed</li>
          </ul>
          <p><strong>Typical</strong>: Expected crowding most of the time</p>
          <p><strong>Peak</strong>: Crowding during busy periods (90th percentile)</p>
        </div>
      )}

      <div className="stop-selectors">
        <label>
          <span className="label-text">
            <span className="label-icon">🟢</span> From
          </span>
          {stopsLoading ? (
            <div className="loading-input">Loading stations...</div>
          ) : (
            <select
              value={selection.origin_stop}
              onChange={(e) => updateField('origin_stop', e.target.value)}
            >
              {!stops.find((s) => s.stop_id === selection.origin_stop) && (
                <option value={selection.origin_stop}>{selection.origin_stop}</option>
              )}
              {stops.map((stop) => (
                <option key={stop.stop_id} value={stop.stop_id}>
                  {stop.stop_name}
                </option>
              ))}
            </select>
          )}
        </label>

        <button className="swap-btn" onClick={swapStops} title="Swap origin and destination">
          ⇅
        </button>

        <label>
          <span className="label-text">
            <span className="label-icon">🔴</span> To
          </span>
          {stopsLoading ? (
            <div className="loading-input">Loading stations...</div>
          ) : (
            <select
              value={selection.dest_stop}
              onChange={(e) => updateField('dest_stop', e.target.value)}
            >
              {!stops.find((s) => s.stop_id === selection.dest_stop) && (
                <option value={selection.dest_stop}>{selection.dest_stop}</option>
              )}
              {stops.map((stop) => (
                <option key={stop.stop_id} value={stop.stop_id}>
                  {stop.stop_name}
                </option>
              ))}
            </select>
          )}
        </label>
      </div>

      <label className="horizon-label">
        <span className="label-text">⏱️ Forecast horizon</span>
        <div className="horizon-buttons">
          {horizonOptions.map((opt) => (
            <button
              key={opt.value}
              className={`horizon-btn ${selection.horizon_min === opt.value ? 'active' : ''}`}
              onClick={() => updateField('horizon_min', opt.value)}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </label>

      <div className="route-summary">
        <span className="route-text">{originName}</span>
        <span className="route-arrow">→</span>
        <span className="route-text">{destName}</span>
      </div>

      <div className="crowding-card">
        <div className="crowding-header">
          <span>Crowding Forecast</span>
          <span className="crowding-horizon">in {selection.horizon_min} min</span>
        </div>

        {prediction ? (
          <>
            <div className="crowding-main">
              <div className="crowding-stat">
                <div className="crowding-emoji">{typicalLevel.emoji}</div>
                <div className="crowding-value" style={{ color: typicalLevel.color }}>
                  {typicalLevel.label}
                </div>
                <div className="crowding-sublabel">Typical</div>
                <div className="crowding-score">{(typicalCrowding * 100).toFixed(0)}%</div>
              </div>
              
              <div className="crowding-divider">
                <span className="divider-line"></span>
                <span className="divider-text">to</span>
                <span className="divider-line"></span>
              </div>
              
              <div className="crowding-stat">
                <div className="crowding-emoji">{peakLevel.emoji}</div>
                <div className="crowding-value" style={{ color: peakLevel.color }}>
                  {peakLevel.label}
                </div>
                <div className="crowding-sublabel">Peak</div>
                <div className="crowding-score">{(peakCrowding * 100).toFixed(0)}%</div>
              </div>
            </div>

            {/* Crowding gauge */}
            <div className="crowding-gauge">
              <div className="gauge-track">
                <div className="gauge-segment low" style={{ width: '25%' }}></div>
                <div className="gauge-segment moderate" style={{ width: '25%' }}></div>
                <div className="gauge-segment high" style={{ width: '25%' }}></div>
                <div className="gauge-segment very-high" style={{ width: '25%' }}></div>
                
                {/* Typical marker */}
                <div 
                  className="gauge-marker typical-marker"
                  style={{ left: `${Math.min(typicalCrowding * 100, 98)}%` }}
                  title={`Typical: ${(typicalCrowding * 100).toFixed(0)}%`}
                >
                  <div className="marker-line"></div>
                  <div className="marker-label">T</div>
                </div>
                
                {/* Peak marker */}
                <div 
                  className="gauge-marker peak-marker"
                  style={{ left: `${Math.min(peakCrowding * 100, 98)}%` }}
                  title={`Peak: ${(peakCrowding * 100).toFixed(0)}%`}
                >
                  <div className="marker-line"></div>
                  <div className="marker-label">P</div>
                </div>
              </div>
              <div className="gauge-labels">
                <span>Empty</span>
                <span>Packed</span>
              </div>
            </div>

            <div className="crowding-tip">
              {typicalCrowding < 0.25 ? (
                <p>✨ <strong>Great time to travel!</strong> Expect plenty of available seats.</p>
              ) : typicalCrowding < 0.5 ? (
                <p>👍 <strong>Manageable crowding.</strong> You may need to stand briefly.</p>
              ) : typicalCrowding < 0.75 ? (
                <p>⚠️ <strong>Busy period.</strong> Consider waiting {selection.horizon_min + 10} min for less crowding.</p>
              ) : (
                <p>🚨 <strong>Very crowded!</strong> Delays likely. Consider alternative routes.</p>
              )}
            </div>
          </>
        ) : (
          <div className="crowding-loading">Calculating...</div>
        )}
      </div>

      {/* TFT Forecast Section */}
      {forecastData && forecastData.forecasts && forecastData.forecasts.length > 0 && (
        <div className="forecast-card">
          <div className="forecast-header">
            <h3>📈 Multi-Horizon Forecast (TFT)</h3>
            <span className="forecast-subtitle">Time series prediction</span>
          </div>

          <div className="forecast-horizons">
            {forecastData.forecasts.map((forecast) => {
              const level = getCrowdingLevel(forecast.p50);
              const isSelected = forecast.horizon_min === selection.horizon_min;

              return (
                <div
                  key={forecast.horizon_min}
                  className={`forecast-horizon-item ${isSelected ? 'selected' : ''}`}
                >
                  <div className="forecast-time">{forecast.horizon_min} min</div>
                  <div className="forecast-level">
                    <span className="forecast-emoji">{level.emoji}</span>
                    <span className="forecast-value" style={{ color: level.color }}>
                      {(forecast.p50 * 100).toFixed(0)}%
                    </span>
                  </div>
                  <div className="forecast-range">
                    <span className="range-label">Range:</span>
                    <span className="range-values">
                      {(forecast.p50 * 100).toFixed(0)}% - {(forecast.p90 * 100).toFixed(0)}%
                    </span>
                  </div>
                </div>
              );
            })}
          </div>

          {forecastLoading && (
            <div className="forecast-loading">Updating forecast...</div>
          )}

          <div className="forecast-info">
            <p>💡 <strong>TFT Model:</strong> Temporal Fusion Transformer analyzes historical patterns to predict crowding trends across multiple time horizons.</p>
          </div>
        </div>
      )}
    </div>
  );
};

export default ControlPanel;
