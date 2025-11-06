"""
Facebook Users Search API - Optimized for Render.com
"""

from fastapi import FastAPI, HTTPException, Depends, status, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import secrets
from elasticsearch import Elasticsearch
import os

# ==================== הגדרות ====================

# Elasticsearch Configuration
ES_HOST = os.getenv('ES_HOST', 'http://72.61.17.220:9200')
ES_USER = os.getenv('ES_USER', 'user1')
ES_PASSWORD = os.getenv('ES_PASSWORD', 'userfbus1025')
DEFAULT_INDEX_PATTERN = os.getenv('INDEX_PATTERN', "fbus*,smfb*,smfbgermania*")

# API Authentication
API_USERNAME = os.getenv('API_USERNAME', 'admin')
API_PASSWORD = os.getenv('API_PASSWORD', 'admin123')

# Server Configuration
PORT = int(os.getenv('PORT', 10000))  # Render uses PORT env variable

# ==================== FastAPI App ====================

app = FastAPI(
    title="Facebook Users Search API",
    version="1.0.0",
    description="🔍 API for searching Facebook users in Elasticsearch"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBasic()

# Elasticsearch Connection
try:
    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASSWORD),
        timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )
    print(f"✓ Elasticsearch configured: {ES_HOST}")
except Exception as e:
    print(f"✗ Elasticsearch error: {e}")
    es = None

# ==================== Models ====================

class PersonResponse(BaseModel):
    uid: Optional[int] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    gender: Optional[str] = None
    location: Optional[str] = None
    hometown: Optional[str] = None
    relationship_status: Optional[str] = None

class UIDSearchRequest(BaseModel):
    uids: List[int] = Field(..., min_items=1, max_items=1000)
    index_pattern: Optional[str] = DEFAULT_INDEX_PATTERN

class SearchResponse(BaseModel):
    success: bool
    count: int
    results: List[PersonResponse]
    query_time_ms: Optional[float] = None

class HealthResponse(BaseModel):
    status: str
    elasticsearch_connected: bool
    elasticsearch_host: str
    api_version: str
    timestamp: str

# ==================== Authentication ====================

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, API_USERNAME)
    correct_password = secrets.compare_digest(credentials.password, API_PASSWORD)
    
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def check_elasticsearch():
    if es is None or not es.ping():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Elasticsearch unavailable"
        )

# ==================== Endpoints ====================

@app.get("/")
async def root():
    return {
        "message": "Facebook Users Search API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    es_connected = False
    if es is not None:
        try:
            es_connected = es.ping()
        except:
            pass
    
    return HealthResponse(
        status="healthy" if es_connected else "degraded",
        elasticsearch_connected=es_connected,
        elasticsearch_host=ES_HOST,
        api_version="1.0.0",
        timestamp=datetime.now().isoformat()
    )

@app.get("/search/uid/{uid}", response_model=SearchResponse, dependencies=[Depends(verify_credentials)])
async def search_by_uid(
    uid: int,
    index_pattern: str = Query(DEFAULT_INDEX_PATTERN)
):
    check_elasticsearch()
    start_time = datetime.now()
    
    try:
        response = es.search(
            index=index_pattern,
            body={"query": {"term": {"uid": uid}}}
        )
        
        results = [PersonResponse(**hit['_source']) for hit in response['hits']['hits']]
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return SearchResponse(
            success=True,
            count=len(results),
            results=results,
            query_time_ms=round(query_time, 2)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search/uids", response_model=SearchResponse, dependencies=[Depends(verify_credentials)])
async def search_multiple_uids(request: UIDSearchRequest):
    check_elasticsearch()
    start_time = datetime.now()
    
    try:
        response = es.search(
            index=request.index_pattern,
            body={
                "query": {"terms": {"uid": request.uids}},
                "size": len(request.uids)
            }
        )
        
        results = [PersonResponse(**hit['_source']) for hit in response['hits']['hits']]
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return SearchResponse(
            success=True,
            count=len(results),
            results=results,
            query_time_ms=round(query_time, 2)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search/email/{email}", response_model=SearchResponse, dependencies=[Depends(verify_credentials)])
async def search_by_email(
    email: str,
    index_pattern: str = Query(DEFAULT_INDEX_PATTERN)
):
    check_elasticsearch()
    start_time = datetime.now()
    
    try:
        response = es.search(
            index=index_pattern,
            body={"query": {"term": {"email.keyword": email}}}
        )
        
        results = [PersonResponse(**hit['_source']) for hit in response['hits']['hits']]
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return SearchResponse(
            success=True,
            count=len(results),
            results=results,
            query_time_ms=round(query_time, 2)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats", dependencies=[Depends(verify_credentials)])
async def get_statistics(index_pattern: str = Query(DEFAULT_INDEX_PATTERN)):
    check_elasticsearch()
    
    try:
        total_count = es.count(index=index_pattern)['count']
        
        gender_agg = es.search(
            index=index_pattern,
            body={
                "size": 0,
                "aggs": {
                    "genders": {
                        "terms": {"field": "gender.keyword", "size": 10}
                    }
                }
            }
        )
        
        return {
            "success": True,
            "total_documents": total_count,
            "genders": [
                {"gender": bucket['key'], "count": bucket['doc_count']}
                for bucket in gender_agg['aggregations']['genders']['buckets']
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Startup ====================

if __name__ == "__main__":
    import uvicorn
    print(f"""
╔════════════════════════════════════════════╗
║  Facebook Users Search API - Render.com   ║
║  Port: {PORT}                                  ║
║  Docs: /docs                               ║
╚════════════════════════════════════════════╝
    """)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
