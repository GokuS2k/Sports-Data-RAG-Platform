# FastAPI entry point placeholder

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Backend is up"}
