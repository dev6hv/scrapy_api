# main.py

import asyncio
import uvloop
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scraper.spiders.site_scraper import SiteMapScraper, WebsiteLinksScraper, ContactScraper
from crochet import setup, wait_for
import time
import logging
from twisted.internet import asyncioreactor
from twisted.internet.defer import inlineCallbacks, returnValue

# Use uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Install Twisted reactor
try:
    asyncioreactor.install()
except Exception as e:
    if "already installed" not in str(e).lower():
        raise

setup()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="Website Scraper API",
    description="API for scraping sitemap, links, and contact details from websites.",
    version="1.0.0"
)

@wait_for(timeout=180.0)
@inlineCallbacks
def run_spider(spider_class, **kwargs):
    settings = get_project_settings()

    settings.set('LOG_LEVEL', 'INFO')
    settings.set('CONCURRENT_REQUESTS', 4)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 2)
    settings.set('DOWNLOAD_DELAY', 1)
    settings.set('DOWNLOAD_TIMEOUT', 30)
    settings.set('AUTOTHROTTLE_ENABLED', True)
    settings.set('AUTOTHROTTLE_START_DELAY', 1)
    settings.set('AUTOTHROTTLE_MAX_DELAY', 10)
    settings.set('COOKIES_ENABLED', False)
    settings.set('RANDOMIZE_DOWNLOAD_DELAY', 0.5)
    settings.set('ROBOTSTXT_OBEY', True)
    settings.set('REQUEST_FINGERPRINTER_IMPLEMENTATION', '2.7')
    settings.set('RETRY_HTTP_CODES', [500, 502, 503, 504, 408, 429])
    settings.set('USER_AGENT', 'Mozilla/5.0 (compatible; ScrapyBot/1.0; +http://www.yourdomain.com/bot)')

    runner = CrawlerRunner(settings)
    result = yield runner.crawl(spider_class, **kwargs)
    returnValue(result)

@app.get("/")
def root():
    return {
        "message": "Website Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "/sitemap": "Scrape entire website using sitemap",
            "/links": "Extract all links from a specific page", 
            "/contact": "Find contact information (email, phone)"
        }
    }

@app.get("/sitemap")
def run_sitemap_scraper(project_url: str = Query(...)):
    try:
        if not project_url.startswith(('http://', 'https://')):
            project_url = 'https://' + project_url

        logger.info(f"Starting sitemap scraper for {project_url}")
        result = run_spider(SiteMapScraper, project_url=project_url)

        if not result:
            raise HTTPException(status_code=500, detail="No data returned")

        return JSONResponse(content={
            "status": "success",
            "scraper_type": "sitemap",
            "target_url": project_url,
            "pages_found": len(result),
            "data": result
        })

    except Exception as e:
        logger.error(f"Sitemap scraper error: {str(e)}")
        return JSONResponse(content={"status": "error", "error_message": str(e)}, status_code=500)

@app.get("/links")
def run_links_scraper(url: str = Query(...)):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        logger.info(f"Starting links scraper for {url}")
        result = run_spider(WebsiteLinksScraper, url=url)

        if not result:
            raise HTTPException(status_code=500, detail="No data returned")

        return JSONResponse(content={
            "status": "success",
            "scraper_type": "links",
            "target_url": url,
            "items_found": len(result),
            "data": result
        })

    except Exception as e:
        logger.error(f"Links scraper error: {str(e)}")
        return JSONResponse(content={"status": "error", "error_message": str(e)}, status_code=500)

@app.get("/contact")
def run_contact_scraper(url: str = Query(...)):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        logger.info(f"Starting contact scraper for {url}")
        result = run_spider(ContactScraper, url=url)

        if not result:
            raise HTTPException(status_code=500, detail="No data returned")

        return JSONResponse(content={
            "status": "success",
            "scraper_type": "contact",
            "target_url": url,
            "contact_info_found": len(result),
            "data": result
        })

    except Exception as e:
        logger.error(f"Contact scraper error: {str(e)}")
        return JSONResponse(content={"status": "error", "error_message": str(e)}, status_code=500)

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "timestamp": int(time.time()),
        "service": "Website Scraper API"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=9000, reload=True, log_level="info")
