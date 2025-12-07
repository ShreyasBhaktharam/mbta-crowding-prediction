import DeckGL from '@deck.gl/react';
import { ScatterplotLayer } from '@deck.gl/layers';
import { Map } from 'react-map-gl/maplibre';
import { cellToLatLng } from 'h3-js';
import type { CrowdingPoint, PredictionSelection } from '../types';
import 'maplibre-gl/dist/maplibre-gl.css';

// Free Carto basemap (no API key required)
const CARTO_BASEMAP = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';

interface Props {
  features: CrowdingPoint[];
  selection: PredictionSelection;
  onSelect: (origin: string, dest: string) => void;
}

const MapView = ({ features, selection, onSelect }: Props) => {
  // Get crowding color based on score (0-1 scale)
  const getCrowdingColor = (score: number): [number, number, number, number] => {
    if (score < 0.25) return [16, 185, 129, 200];  // Green - Low
    if (score < 0.5) return [245, 158, 11, 200];   // Yellow - Moderate
    if (score < 0.75) return [249, 115, 22, 200];  // Orange - High
    return [239, 68, 68, 200];                      // Red - Very High
  };

  const layers = [
    new ScatterplotLayer<CrowdingPoint>({
      id: 'crowding-layer',
      data: features,
      pickable: true,
      getPosition: (d) => {
        if (d.h3) {
          try {
            const [lat, lon] = cellToLatLng(d.h3);
            return [lon, lat];
          } catch {
            return [-71.0589, 42.3601];
          }
        }
        return [-71.0589, 42.3601];
      },
      getFillColor: (d) => getCrowdingColor(d.p90),
      getRadius: (d) => 300 + (d.p90 * 500),
      radiusMinPixels: 8,
      radiusMaxPixels: 50,
      onClick: ({ object }) => {
        if (object) {
          onSelect(object.origin_stop, object.dest_stop);
        }
      },
    }),
  ];

  return (
    <DeckGL 
      controller 
      layers={layers} 
      initialViewState={{ 
        longitude: -71.0589, 
        latitude: 42.3601, 
        zoom: 12,
        pitch: 0,
        bearing: 0,
      }}
    >
      <Map mapStyle={CARTO_BASEMAP} />
    </DeckGL>
  );
};

export default MapView;
