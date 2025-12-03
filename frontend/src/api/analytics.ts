import axios from "axios";
import { API_BASE } from "./articles";

export interface SentimentPoint {
  time: string;        // ISO string
  positive: number;
  neutral: number;
  negative: number;
}

export interface KeywordCount {
  keyword: string;
  count: number;
}

export async function fetchSentimentOverTime(hours = 24, bucket: "hour" | "day" = "hour") {
  const response = await axios.get(`${API_BASE}/analytics/sentiment-over-time`, {
    params: { hours, bucket },
  });
  return response.data.items as SentimentPoint[];
}

export async function fetchTopKeywords(limit = 10, hours = 24) {
  const response = await axios.get(`${API_BASE}/analytics/top-keywords`, {
    params: { limit, hours },
  });
  return response.data.items as KeywordCount[];
}
