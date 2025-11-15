import { useEffect, useState } from 'react';
import type { CrowdingPoint } from '../types';

const useCrowdingStream = (apiBase: string) => {
  const [points, setPoints] = useState<CrowdingPoint[]>([]);

  useEffect(() => {
    const source = new EventSource(`${apiBase}/crowding_map`);
    source.onmessage = (event) => {
      try {
        const payload: CrowdingPoint[] = JSON.parse(event.data);
        setPoints(payload);
      } catch (err) {
        console.error('Failed to parse SSE payload', err);
      }
    };
    source.onerror = (err) => {
      console.warn('Crowding stream error', err);
      source.close();
    };
    return () => source.close();
  }, [apiBase]);

  return { points };
};

export default useCrowdingStream;
