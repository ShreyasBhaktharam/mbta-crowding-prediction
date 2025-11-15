import type { PredictionSelection } from '../types';

interface Props {
  selection: PredictionSelection;
  prediction: { p50: number; p90: number } | null;
  onChange: (sel: PredictionSelection) => void;
}

const ControlPanel = ({ selection, prediction, onChange }: Props) => {
  const updateField = (field: keyof PredictionSelection, value: string | number) => {
    onChange({ ...selection, [field]: value });
  };

  return (
    <div className="panel">
      <h2>Prediction Controls</h2>
      <label>
        Origin stop
        <input value={selection.origin_stop} onChange={(e) => updateField('origin_stop', e.target.value)} />
      </label>
      <label>
        Destination stop
        <input value={selection.dest_stop} onChange={(e) => updateField('dest_stop', e.target.value)} />
      </label>
      <label>
        Horizon (min)
        <input
          type="number"
          min={5}
          max={60}
          value={selection.horizon_min}
          onChange={(e) => updateField('horizon_min', Number(e.target.value))}
        />
      </label>
      <div className="prediction">
        <span>p50: {prediction?.p50?.toFixed(2) ?? '—'}</span>
        <span>p90: {prediction?.p90?.toFixed(2) ?? '—'}</span>
      </div>
    </div>
  );
};

export default ControlPanel;
