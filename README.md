📝 Overview

The Real-Time News Analytics Platform is a fully containerized microservices system designed to ingest news from external sources, enrich articles with NLP sentiment analysis, aggregate insights, and present them in a live, interactive dashboard.

This project demonstrates:

* Modern microservice architecture

* Real-time data pipelines using Kafka

* Sentiment analysis, keyword extraction, and trend analytics

* Event-driven communication

* A responsive React + Material-UI dashboard

* Scalable container-based deployment with Docker Compose

<img width="1498" height="995" alt="image" src="https://github.com/user-attachments/assets/da50a410-78ed-46c3-94c6-3117211fc679" />

<br>

🛠 Technologies Used
Backend

* FastAPI

* Kafka + aiokafka

* MongoDB

* Python 3.11

* NLP sentiment analysis (rule-based)

Frontend

* React (Vite + TypeScript)

* Material UI v5

* Recharts.js for charting

* Axios

DevOps & Infrastructure

* Docker Compose

* Multi-container architecture

* Hot reload enabled via bind mounts


🚀 Features
🔍 Real-Time News Ingestion

* Pulls live headlines from multiple sources (BBC, CNN, Reuters, The Verge, etc.) using the NewsAPI.

* Configurable polling interval via environment variables.

* Deduplication logic ensures unique article ingestion.

* Sends raw articles into Kafka (news.raw topic).

🤖 NLP-Based Enrichment

* Sentiment classification (positive / neutral / negative).

* Keyword extraction.

* Timestamp normalization.

* Enhanced article objects are pushed to Kafka (news.enriched).

📈 Analytics Engine

Provides:

* Sentiment distribution (positive/neutral/negative)

* Sentiment trend over time (hourly buckets)

* Top keywords (last 24h)

* Fully REST-based API for the frontend dashboard

🖥 Live Dashboard

Built in React + TypeScript + Material UI, featuring:

* Realtime mode toggle

* Scrollable list of the latest enriched articles

* Pie chart for sentiment distribution

* Horizontal bar chart for top keywords

* Time-series chart for sentiment trends

* Automatic refresh every 15 seconds (configurable)

* Fully responsive layout for desktop/mobile

🧱 Architecture Highlights

* Kafka for decoupled event-driven pipeline

* FastAPI microservices

* MongoDB for storing enriched articles and analytics data

* Docker Compose orchestration

* Modular service boundaries (ingestion, enrichment, analytics, gateway)
