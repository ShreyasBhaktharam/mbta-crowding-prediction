export type CrowdingPoint = {
  origin_stop: string;
  dest_stop: string;
  horizon_min: number;
  h3: string;
  rolling_mean: number;
  p50: number;
  p90: number;
};

export type PredictionSelection = {
  origin_stop: string;
  dest_stop: string;
  horizon_min: number;
};
