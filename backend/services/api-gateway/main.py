from fastapi import FastAPI

app = FastAPI(title="News Analytics API Gateway")


@app.get("/api/health")
def health_check():
    return {"status": "ok", "service": "api-gateway"}
