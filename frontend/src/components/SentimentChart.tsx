import { FC } from "react";
import { PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from "recharts";

type SentimentCounts = {
  positive: number;
  neutral: number;
  negative: number;
};

interface Props {
  counts: SentimentCounts;
}

const COLORS = ["#2e7d32", "#0288d1", "#c62828"]; // green, blue, red

export const SentimentChart: FC<Props> = ({ counts }) => {
  const data = [
    { name: "Positive", value: counts.positive },
    { name: "Neutral", value: counts.neutral },
    { name: "Negative", value: counts.negative },
  ];

  const total = data.reduce((sum, d) => sum + d.value, 0);
  if (total === 0) {
    return null; // nothing to show yet
  }

  return (
    <ResponsiveContainer width="100%" height={200}>
      <PieChart>
        <Pie
          data={data}
          dataKey="value"
          nameKey="name"
          cx="50%"
          cy="45%"
          outerRadius={70}
          labelLine={false}
        >
          {data.map((entry, index) => (
            <Cell key={entry.name} fill={COLORS[index]} />
          ))}
        </Pie>
        <Tooltip />
        <Legend verticalAlign="bottom" height={24} />
      </PieChart>
    </ResponsiveContainer>
  );
};
