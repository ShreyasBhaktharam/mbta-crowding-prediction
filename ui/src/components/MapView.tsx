import DeckGL from '@deck.gl/react';
import { ScatterplotLayer } from '@deck.gl/layers';
import { Map } from 'react-map-gl';
import { cellToLatLng } from 'h3-js';
import type { CrowdingPoint, PredictionSelection } from '../types';

const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_TOKEN || '';

interface Props {
  features: CrowdingPoint[];
  selection: PredictionSelection;
  onSelect: (origin: string, dest: string) => void;
}

const MapView = ({ features, selection, onSelect }: Props) => {
  const layers = [
    new ScatterplotLayer<CrowdingPoint>({
      id: 'crowding-layer',
      data: features,
      pickable: true,
      getPosition: (d) => {
        if (d.h3) {
          const [lat, lon] = cellToLatLng(d.h3);
          return [lon, lat];
        }
        return [-71.0589, 42.3601];
      },
      getFillColor: (d) => (d.p90 > 5 ? [220, 50, 50, 200] : [60, 160, 220, 180]),
      getRadius: (d) => 400 + d.p90 * 40,
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
      initialViewState={{ longitude: -71.0589, latitude: 42.3601, zoom: 11 }}
      style={{ width: '100%', height: '100%' }}
    >
      <Map
        mapboxAccessToken={MAPBOX_TOKEN}
        mapStyle="mapbox://styles/mapbox/light-v11"
        style={{ width: '100%', height: '100%' }}
      />
    </DeckGL>
  );
};

export default MapView;
