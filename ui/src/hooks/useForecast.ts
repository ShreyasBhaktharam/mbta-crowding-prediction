import { useEffect, useState } from 'react';

export interface ForecastHorizon {
  horizon_min: number;
  p50: number;
  p90: number;
}

export interface ForecastData {
  origin_stop: string;
  dest_stop: string;
  origin_name?: string;
  dest_name?: string;
  forecasts: ForecastHorizon[];
}

interface UseForecastOptions {
  apiBase: string;
  originStop: string;
  destStop: string;
  horizonMin?: number;
  enabled?: boolean;
  refreshInterval?: number;
}

const useForecast = ({
  apiBase,
  originStop,
  destStop,
  horizonMin = 10,
  enabled = true,
  refreshInterval = 30000, // 30 seconds default
}: UseForecastOptions) => {
  const [forecastData, setForecastData] = useState<ForecastData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled || !originStop || !destStop) {
      setLoading(false);
      return;
    }

    const fetchForecast = async () => {
      try {
        const response = await fetch(`${apiBase}/forecast`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            requests: [
              {
                origin_stop: originStop,
                dest_stop: destStop,
                horizon_min: horizonMin,
              },
            ],
          }),
        });

        if (!response.ok) {
          throw new Error(`Failed to fetch forecast: ${response.status}`);
        }

        const data: ForecastData[] = await response.json();
        if (data.length > 0) {
          setForecastData(data[0]);
          setError(null);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };

    fetchForecast();

    // Set up polling if refresh interval is provided
    if (refreshInterval > 0) {
      const intervalId = setInterval(fetchForecast, refreshInterval);
      return () => clearInterval(intervalId);
    }
  }, [apiBase, originStop, destStop, horizonMin, enabled, refreshInterval]);

  return { forecastData, loading, error };
};

export default useForecast;
