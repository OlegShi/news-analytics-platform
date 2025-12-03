import { useEffect, useState } from "react";
import {
  AppBar,
  Box,
  Container,
  Toolbar,
  Typography,
  Card,
  CardContent,
  Chip,
  Switch,
  FormControlLabel,
} from "@mui/material";

import { fetchEnrichedArticles } from "./api/articles";
import type { EnrichedArticle } from "./api/articles";

import { SentimentChart } from "./components/SentimentChart";
import { SentimentTrendChart } from "./components/SentimentTrendChart";
import { TopKeywordsChart } from "./components/TopKeywordsChart";

import {
  fetchSentimentOverTime,
  fetchTopKeywords,
} from "./api/analytics";
import type { SentimentPoint, KeywordCount } from "./api/analytics";

type SentimentCounts = {
  positive: number;
  neutral: number;
  negative: number;
};

function App() {
  const [articles, setArticles] = useState<EnrichedArticle[]>([]);
  const [sentimentCounts, setSentimentCounts] = useState<SentimentCounts>({
    positive: 0,
    neutral: 0,
    negative: 0,
  });
  const [sentimentTrend, setSentimentTrend] = useState<SentimentPoint[]>([]);
  const [topKeywords, setTopKeywords] = useState<KeywordCount[]>([]);
  const [autoRefresh, setAutoRefresh] = useState(true);

  // === ONE effect that loads everything (articles + analytics) ===
  useEffect(() => {
    let timer: number | undefined;

    const loadAll = async () => {
      try {
        // Fetch everything in parallel
        const [articlesData, trendData, keywordsData] = await Promise.all([
          fetchEnrichedArticles(50),
          fetchSentimentOverTime(24, "hour"),
          fetchTopKeywords(10, 24),
        ]);

        // 1) Latest enriched articles
        setArticles(articlesData);

        // 2) Sentiment overview counts (from articles)
        const counts: SentimentCounts = {
          positive: 0,
          neutral: 0,
          negative: 0,
        };
        for (const a of articlesData) {
          const label = a.sentiment.label as keyof SentimentCounts;
          if (counts[label] !== undefined) {
            counts[label] += 1;
          }
        }
        setSentimentCounts(counts);

        // 3) Sentiment over time
        setSentimentTrend(trendData);

        // 4) Top keywords
        setTopKeywords(keywordsData);
      } catch (err) {
        console.error("Failed to load dashboard data", err);
      }
    };

    // initial load
    loadAll();

    // periodic refresh
    if (autoRefresh) {
      // every 15 seconds – adjust if you want
      timer = window.setInterval(loadAll, 15000);
    }

    // cleanup on unmount / when autoRefresh changes
    return () => {
      if (timer) {
        window.clearInterval(timer);
      }
    };
  }, [autoRefresh]);

  return (
    <>
      <AppBar position="static">
        <Toolbar sx={{ display: "flex", justifyContent: "space-between" }}>
          <Typography variant="h6">
            Real-Time News Analytics Dashboard
          </Typography>

          <FormControlLabel
            control={
              <Switch
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                color="secondary"
              />
            }
            label="Realtime mode"
          />
        </Toolbar>
      </AppBar>

      <Container sx={{ mt: 2, mb: 3 }}>
        <Box
          sx={{
            display: "grid",
            gridTemplateColumns: { xs: "1fr", md: "4fr 5fr" },
            columnGap: 3,
            rowGap: 3,
            alignItems: "flex-start",
          }}
        >
          {/* LEFT: Latest Enriched Articles (scrollable on desktop) */}
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              minWidth: 0,
            }}
          >
            <Typography variant="h5" gutterBottom>
              Latest Enriched Articles
            </Typography>

            <Box
              sx={{
                maxHeight: { xs: "none", md: "calc(100vh - 140px)" },
                overflowY: { xs: "visible", md: "auto" },
                pr: { md: 1 },
              }}
            >
              {articles.map((article) => (
                <Card key={article.id} sx={{ mb: 2 }}>
                  <CardContent>
                    <Typography variant="h6">{article.title}</Typography>
                    <Typography variant="body2" color="text.secondary">
                      {article.summary}
                    </Typography>

                    <Box sx={{ mt: 1 }}>
                      <Chip
                        label={`Sentiment: ${article.sentiment.label}`}
                        color={
                          article.sentiment.label === "positive"
                            ? "success"
                            : article.sentiment.label === "negative"
                            ? "error"
                            : "info"
                        }
                        sx={{ mr: 1 }}
                      />
                      {article.keywords.map((kw) => (
                        <Chip key={kw} label={kw} size="small" sx={{ mr: 1 }} />
                      ))}
                    </Box>

                    <Typography
                      variant="caption"
                      sx={{ display: "block", mt: 1 }}
                    >
                      Source: {article.source}
                    </Typography>
                    <Typography variant="caption" sx={{ display: "block" }}>
                      Published: {article.published_at ?? "N/A"}
                    </Typography>
                  </CardContent>
                </Card>
              ))}
            </Box>
          </Box>

          {/* RIGHT: charts grid */}
          <Box
            sx={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gridAutoRows: "minmax(260px, auto)",
              gap: 2,
              minWidth: 0,
            }}
          >
            {/* Sentiment Overview */}
            <Box
              sx={{
                borderRadius: 2,
                border: "1px solid #eee",
                p: 1.5,
              }}
            >
              <Typography variant="subtitle1" gutterBottom>
                Sentiment Overview
              </Typography>
              <SentimentChart counts={sentimentCounts} />
            </Box>

            {/* Top Keywords */}
            <Box
              sx={{
                borderRadius: 2,
                border: "1px solid #eee",
                p: 1.5,
              }}
            >
              <Typography variant="subtitle1" gutterBottom>
                Top Keywords (last 24h)
              </Typography>
              <TopKeywordsChart data={topKeywords} />
            </Box>

            {/* Sentiment Over Time – spans full width */}
            <Box
              sx={{
                borderRadius: 2,
                border: "1px solid #eee",
                p: 1.5,
                gridColumn: "1 / span 2",
              }}
            >
              <Typography variant="subtitle1" gutterBottom>
                Sentiment Over Time (last 24h)
              </Typography>
              <SentimentTrendChart data={sentimentTrend} />
            </Box>
          </Box>
        </Box>
      </Container>
    </>
  );
}

export default App;
