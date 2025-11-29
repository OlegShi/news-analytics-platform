import axios from "axios";

export const API_BASE = "http://localhost/api";

export interface EnrichedArticle {
  id: string;
  article_id: string;
  source: string;
  title: string;
  summary: string;
  url: string;
  published_at: string | null;
  fetched_at: string | null;
  normalized_at: string | null;
  enriched_at: string | null;
  sentiment: {
    label: string;
    score: number;
  };
  keywords: string[];
}

export async function fetchEnrichedArticles(limit = 20) {
  const response = await axios.get(`${API_BASE}/articles/enriched`, {
    params: { limit },
  });
  return response.data.items as EnrichedArticle[];
}
