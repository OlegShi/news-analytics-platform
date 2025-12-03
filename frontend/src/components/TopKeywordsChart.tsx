import { FC } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import type { KeywordCount } from "../api/analytics";

interface Props {
  data: KeywordCount[];
}

export const TopKeywordsChart: FC<Props> = ({ data }) => {
  if (!data.length) return null;

  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={data} layout="vertical">
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis type="number" />
        <YAxis type="category" dataKey="keyword" width={100} />
        <Tooltip />
        <Bar dataKey="count" name="Occurrences" fill="#0288d1" />
      </BarChart>
    </ResponsiveContainer>
  );
};
