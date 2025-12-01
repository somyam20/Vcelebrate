from fastapi import FastAPI
from contextlib import asynccontextmanager
from src.routes.upload import router as upload_router
from src.routes.query import router as query_router
from src.utils.db import async_init_db
from config.config import initialize_config, cleanup_config
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup
    logger.info("=" * 80)
    logger.info("APPLICATION STARTUP")
    logger.info("=" * 80)
    
    try:
        # Initialize database
        logger.info("Initializing database...")
        await async_init_db()
        logger.info("✓ Database initialized")
        
        # Initialize model config (DB pool, etc.)
        logger.info("Initializing model configuration...")
        await initialize_config()
        logger.info("✓ Model configuration initialized")
        
        logger.info("=" * 80)
        logger.info("✓ APPLICATION READY")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.exception(f"✗ Startup failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("=" * 80)
    logger.info("APPLICATION SHUTDOWN")
    logger.info("=" * 80)
    
    try:
        # Cleanup model config (close DB pool, etc.)
        logger.info("Cleaning up model configuration...")
        await cleanup_config()
        logger.info("✓ Model configuration cleaned up")
        
        logger.info("=" * 80)
        logger.info("✓ APPLICATION SHUTDOWN COMPLETE")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.exception(f"✗ Shutdown failed: {e}")


app = FastAPI(
    title="Milestone & Inventory Management System",
    description="Upload and query milestone, welcome kit, and inventory data",
    version="1.0.0",
    lifespan=lifespan
)

# Include routers
app.include_router(upload_router, prefix="/api", tags=["Upload"])
app.include_router(query_router, prefix="/api", tags=["Query"])


@app.get("/", tags=["Root"])
def root():
    return {
        "message": "Milestone & Inventory Management API",
        "version": "1.0.0",
        "endpoints": {
            "upload": "/api/upload",
            "query": "/api/query",
            "categories": "/api/categories",
            "health": "/health"
        }
    }


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)