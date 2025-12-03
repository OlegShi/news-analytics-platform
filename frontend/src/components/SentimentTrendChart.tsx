import { FC } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import type { SentimentPoint } from "../api/analytics";

interface Props {
  data: SentimentPoint[];
}

export const SentimentTrendChart: FC<Props> = ({ data }) => {
  if (!data.length) return null;

  // Shorten ISO time for x-axis
  const chartData = data.map((d) => ({
    ...d,
    timeLabel: d.time.replace("T", " ").slice(0, 16), // "YYYY-MM-DD HH:MM"
  }));

  return (
    <ResponsiveContainer width="100%" height={260}>
      <LineChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="timeLabel" />
        <YAxis allowDecimals={false} />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="positive" name="Positive" stroke="#2e7d32" />
        <Line type="monotone" dataKey="neutral" name="Neutral" stroke="#0288d1" />
        <Line type="monotone" dataKey="negative" name="Negative" stroke="#c62828" />
      </LineChart>
    </ResponsiveContainer>
  );
};
