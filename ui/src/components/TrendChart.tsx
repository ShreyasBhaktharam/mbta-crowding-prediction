import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { CrowdingPoint } from '../types';

interface Props {
  data: CrowdingPoint[];
}

const TrendChart = ({ data }: Props) => (
  <div className="panel">
    <h3>Recent p50 trend</h3>
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={data}>
        <XAxis dataKey="origin_stop" hide />
        <YAxis hide domain={['auto', 'auto']} />
        <Tooltip formatter={(value) => `${value} pax`} />
        <Line type="monotone" dataKey="p50" stroke="#0070f3" dot={false} />
      </LineChart>
    </ResponsiveContainer>
  </div>
);

export default TrendChart;
