import { useEffect, useState } from 'react';

export interface Stop {
  stop_id: string;
  stop_name: string;
}

const useStops = (apiBase: string) => {
  const [stops, setStops] = useState<Stop[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchStops = async () => {
      try {
        const response = await fetch(`${apiBase}/stops`);
        if (!response.ok) {
          throw new Error(`Failed to fetch stops: ${response.status}`);
        }
        const data: Stop[] = await response.json();
        setStops(data);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };
    fetchStops();
  }, [apiBase]);

  const getStopName = (stopId: string): string => {
    const stop = stops.find((s) => s.stop_id === stopId);
    return stop?.stop_name ?? stopId;
  };

  return { stops, loading, error, getStopName };
};

export default useStops;

