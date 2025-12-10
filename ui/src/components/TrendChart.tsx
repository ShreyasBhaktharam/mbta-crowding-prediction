import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis, ReferenceLine, Area, AreaChart } from 'recharts';
import type { CrowdingPoint } from '../types';
import type { ForecastData } from '../hooks/useForecast';

interface Props {
  data: CrowdingPoint[];
  forecastData?: ForecastData | null;
}

const TrendChart = ({ data, forecastData }: Props) => {
  // Prepare historical data (backward-looking)
  const historicalData = data.slice(-20).map((point, idx) => ({
    time: -20 + idx,
    p50: point.p50,
    type: 'historical',
    label: `${-20 + idx} min`,
  }));

  // Prepare forecast data (forward-looking)
  const forecastPoints = forecastData?.forecasts?.map((forecast) => ({
    time: forecast.horizon_min,
    p50: forecast.p50,
    p90: forecast.p90,
    type: 'forecast',
    label: `+${forecast.horizon_min} min`,
  })) || [];

  // Add "NOW" point at time 0
  const lastHistorical = historicalData.length > 0 ? historicalData[historicalData.length - 1].p50 : 0;
  const nowPoint = {
    time: 0,
    p50: lastHistorical,
    type: 'now',
    label: 'NOW',
  };

  // Combine all data
  const combinedData = [...historicalData, nowPoint, ...forecastPoints];

  return (
    <div className="panel trend-chart-panel">
      <div className="trend-header">
        <h3>Crowding Trend</h3>
        <div className="trend-legend">
          <span className="legend-item">
            <span className="legend-line historical"></span>
            Historical
          </span>
          <span className="legend-item">
            <span className="legend-line forecast"></span>
            Forecast
          </span>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={combinedData} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
          <XAxis
            dataKey="time"
            tickFormatter={(value) => {
              if (value === 0) return 'NOW';
              if (value < 0) return `${value}m`;
              return `+${value}m`;
            }}
            domain={['dataMin', 'dataMax']}
          />
          <YAxis
            domain={[0, 1]}
            tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
            width={45}
          />
          <Tooltip
            formatter={(value: number, name: string) => {
              if (name === 'p50') return [`${(value * 100).toFixed(1)}% (Typical)`, 'Crowding'];
              if (name === 'p90') return [`${(value * 100).toFixed(1)}% (Peak)`, 'Crowding'];
              return [value, name];
            }}
            labelFormatter={(label) => {
              if (label === 0) return 'NOW';
              if (label < 0) return `${label} minutes ago`;
              return `+${label} minutes`;
            }}
          />

          {/* Reference line at NOW */}
          <ReferenceLine
            x={0}
            stroke="#666"
            strokeDasharray="3 3"
            label={{ value: 'NOW', position: 'top', fill: '#666', fontSize: 12 }}
          />

          {/* Historical line (gray) */}
          <Line
            type="monotone"
            dataKey="p50"
            data={[...historicalData, nowPoint]}
            stroke="#94a3b8"
            strokeWidth={2}
            dot={false}
            connectNulls
          />

          {/* Forecast line (blue) */}
          <Line
            type="monotone"
            dataKey="p50"
            data={[nowPoint, ...forecastPoints]}
            stroke="#3b82f6"
            strokeWidth={2}
            dot={{ r: 3 }}
            connectNulls
          />

          {/* Forecast p90 line (light blue, dashed) */}
          {forecastPoints.length > 0 && (
            <Line
              type="monotone"
              dataKey="p90"
              data={[nowPoint, ...forecastPoints]}
              stroke="#60a5fa"
              strokeWidth={1}
              strokeDasharray="5 5"
              dot={false}
              connectNulls
            />
          )}
        </LineChart>
      </ResponsiveContainer>
      <div className="trend-info">
        <p className="trend-description">
          Gray: Recent crowding history | Blue: TFT forecast (solid: p50, dashed: p90)
        </p>
      </div>
    </div>
  );
};

export default TrendChart;
