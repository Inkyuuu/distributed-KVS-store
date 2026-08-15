from fastapi import FastAPI
from app.routes import router

app = FastAPI(
    title="FastAPI REST API",
    version="1.0",
    description="CSE 138 REST API Assignment 4",
)

app.include_router(router)
