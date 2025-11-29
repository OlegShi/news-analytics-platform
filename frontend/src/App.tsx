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
} from "@mui/material";
import { fetchEnrichedArticles } from "./api/articles";
import type { EnrichedArticle } from "./api/articles";
import { SentimentChart } from "./components/SentimentChart";

// === sentiment counts type ===
type SentimentCounts = {
  positive: number;
  neutral: number;
  negative: number;
};

function App() {
  // === React state ===
  const [articles, setArticles] = useState<EnrichedArticle[]>([]);
  const [sentimentCounts, setSentimentCounts] = useState<SentimentCounts>({
    positive: 0,
    neutral: 0,
    negative: 0,
  });

  // === Fetch data on load ===
  useEffect(() => {
    fetchEnrichedArticles(20)
      .then((items) => {
        setArticles(items);

        const counts: SentimentCounts = {
          positive: 0,
          neutral: 0,
          negative: 0,
        };

        for (const a of items) {
          const label = a.sentiment.label as keyof SentimentCounts;
          if (counts[label] !== undefined) {
            counts[label] += 1;
          }
        }

        setSentimentCounts(counts);
      })
      .catch((err) => {
        console.error("Failed to load enriched articles", err);
      });
  }, []);

  return (
    <>
      {/* === TOP BAR === */}
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6">
            Real-Time News Analytics Dashboard
          </Typography>
        </Toolbar>
      </AppBar>

      {/* === MAIN PAGE CONTENT === */}
      <Container sx={{ mt: 4 }}>

        {/* === SENTIMENT CHART SECTION === */}
        <Typography variant="h5" gutterBottom>
          Sentiment Overview
        </Typography>

        <Box sx={{ mb: 4 }}>
          <SentimentChart counts={sentimentCounts} />
        </Box>

        {/* === ARTICLES SECTION === */}
        <Typography variant="h5" gutterBottom>
          Latest Enriched Articles
        </Typography>

        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {articles.map((article) => (
            <Card key={article.id}>
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

                <Typography variant="caption" sx={{ display: "block", mt: 1 }}>
                  Source: {article.source}
                </Typography>

                <Typography variant="caption" sx={{ display: "block" }}>
                  Published: {article.published_at ?? "N/A"}
                </Typography>
              </CardContent>
            </Card>
          ))}
        </Box>
      </Container>
    </>
  );
}

export default App;
