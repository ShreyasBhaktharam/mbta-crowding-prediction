# CityStream UI

A Vite + React + deck.gl dashboard displaying the live MBTA crowding map and quantile predictions streamed from the FastAPI service.

## Prerequisites

- Node.js 18+
- MAPBOX token (set `VITE_MAPBOX_TOKEN`)
- API base (set `VITE_API_BASE`, defaults to `http://localhost:8000`)

## Scripts

```bash
cd ui
npm install
npm run dev        # starts Vite dev server
npm run build      # outputs static assets in ui/dist
npm run preview    # serve the production bundle
```

The FastAPI backend can mount `ui/dist` for static hosting once `npm run build` completes.
