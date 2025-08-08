import scrapy
from scrapy.spiders import SitemapSpider, Spider
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from scrapy.http import Request
import re
import logging
import phonenumbers
import requests
from scrapy import signals

# Configure logging
logging.getLogger('playwright').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

class SiteMapScraper(SitemapSpider):
    name = "site_scraper"
    
    def __init__(self, project_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not project_url:
            raise ValueError("Please provide project_url")
        
        self.project_url = project_url.rstrip('/')
        self.base_domain = urlparse(project_url).netloc
        self.sitemap_urls = []
        self.allowed_domains = [self.base_domain]
        self.visited_urls = set()
        self.excluded_urls = set()
        self.path_exclusions = set()
        self.scraped_data = []  # Store scraped data here
        
        self.link_extractor = LinkExtractor(
            allow_domains=self.allowed_domains,
            deny=[r'.*\.pdf$', r'.*\.doc$', r'.*\.docx$', r'.*\.jpg$', r'.*\.png$', r'.*\.gif$'],
            canonicalize=True
        )
        
        self.logger.info(f"Initialized SiteMapScraper for {project_url}")
        self._discover_sitemap_urls()

    def _discover_sitemap_urls(self):
        potential_sitemaps = [
            f"{self.project_url}/sitemap.xml",
            f"{self.project_url}/sitemap_index.xml",
            f"{self.project_url}/sitemaps.xml"
        ]
        
        robots_sitemap = self._get_sitemap_from_robots()
        self.sitemap_urls = [robots_sitemap] if robots_sitemap else potential_sitemaps
        self.logger.info(f"Sitemap URLs: {self.sitemap_urls}")

    def _get_sitemap_from_robots(self):
        try:
            robots_url = f"{self.project_url}/robots.txt"
            response = requests.get(robots_url, timeout=10)
            if response.status_code == 200:
                for line in response.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sitemap_url = line.split(":", 1)[1].strip()
                        if sitemap_url.startswith(('http://', 'https://')):
                            self.logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
                            return sitemap_url
        except Exception as e:
            self.logger.warning(f"Failed to fetch robots.txt: {e}")
        return None

    def clean_content(self, soup):
        for tag in ['aside', 'nav', 'footer', 'form', 'iframe', 'script', 'svg', 
                   'button', 'select', 'input', 'label', 'source', 'audio', 'video', 'img']:
            for element in soup.find_all(tag):
                element.decompose()
        
        main = (soup.find('main') or 
                soup.find('article') or 
                soup.find('div', class_='content') or 
                soup.find('div', id='content') or 
                soup.body)
        return main

    def is_url_excluded(self, url):
        url = url.rstrip('/')
        return url in self.excluded_urls

    def is_path_exclusion(self, url):
        url = url.rstrip('/')
        for path in self.path_exclusions:
            if url == path or url.startswith(path + '/'):
                return True
        return False

    def handle_error(self, failure):
        self.logger.error(f"Request failed for {failure.request.url}: {str(failure)}")
        self.scraped_data.append({
            "url": failure.request.url,
            "status": "error",
            "error_message": str(failure)
        })

    def parse(self, response):
        current_url = response.url.rstrip('/')
        self.logger.info(f"Parsing URL: {current_url}")
        
        if self.is_url_excluded(current_url) or self.is_path_exclusion(current_url):
            self.logger.info(f"Skipping excluded URL: {current_url}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        cleaned_content = self.clean_content(soup)
        content_text = cleaned_content.get_text(separator='\n').strip() if cleaned_content else ''
        content_text = re.sub(r'\n\s*\n', '\n', content_text)

        page_data = {
            'url': current_url,
            'title': response.xpath('//title/text()').get('').strip(),
            'description': response.xpath("//meta[@name='description']/@content").get('').strip(),
            'h1': response.xpath('//h1[1]/text()').get('').strip(),
            'content_text': content_text,
            'status_code': response.status,
            'word_count': len(content_text.split()) if content_text else 0
        }
        
        self.scraped_data.append(page_data)
        self.logger.info(f"Added page data for {current_url}: {page_data}")

        links = self.link_extractor.extract_links(response)
        for link in links:
            link_url = link.url.rstrip('/')
            if (link_url not in self.visited_urls and 
                not self.is_url_excluded(link_url) and 
                not self.is_path_exclusion(link_url)):
                self.visited_urls.add(link_url)
                self.logger.info(f"Following link: {link_url}")
                yield Request(link_url, callback=self.parse, errback=self.handle_error)

    def start_requests(self):
        for url in self.sitemap_urls:
            self.logger.info(f"Requesting sitemap: {url}")
            yield Request(url, callback=self._parse_sitemap, dont_filter=True, errback=self.handle_error)
        self.logger.info(f"Requesting main URL: {self.project_url}")
        yield Request(self.project_url, callback=self.parse, errback=self.handle_error)

    def _parse_sitemap(self, response):
        self.logger.info(f"Parsing sitemap: {response.url}")
        if response.status == 200:
            soup = BeautifulSoup(response.text, 'xml')
            sitemap_tags = soup.find_all('sitemap')
            for sitemap in sitemap_tags:
                loc = sitemap.find('loc')
                if loc:
                    self.logger.info(f"Found nested sitemap: {loc.text}")
                    yield Request(loc.text, callback=self._parse_sitemap, errback=self.handle_error)
            
            url_tags = soup.find_all('url')
            for url_tag in url_tags:
                loc = url_tag.find('loc')
                if loc:
                    url = loc.text.rstrip('/')
                    if (url not in self.visited_urls and 
                        not self.is_url_excluded(url) and 
                        not self.is_path_exclusion(url)):
                        self.visited_urls.add(url)
                        self.logger.info(f"Found sitemap URL: {url}")
                        yield Request(url, callback=self.parse, errback=self.handle_error)
        else:
            self.logger.warning(f"Sitemap request failed: {response.url}, status: {response.status}")

    def closed(self, reason):
        self.logger.info(f"Spider closed. Scraped data: {len(self.scraped_data)} items")
        return self.scraped_data

class WebsiteLinksScraper(Spider):
    name = "website_links_scraper"
    
    def __init__(self, url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not url:
            raise ValueError("Please provide url")
        
        self.url = url.rstrip('/')
        self.base_domain = urlparse(url).netloc
        self.allowed_domains = [self.base_domain]
        self.link_extractor = LinkExtractor(canonicalize=True, unique=True)
        self.scraped_data = []  # Store scraped data here
        self.logger.info(f"Initialized WebsiteLinksScraper for {url}")

    def start_requests(self):
        self.logger.info(f"Requesting URL: {self.url}")
        yield Request(self.url, callback=self.parse)

    def parse(self, response):
        self.logger.info(f"Parsing URL: {response.url}")
        http_status_code = response.status
        robots_meta = response.xpath("//meta[@name='robots']/@content").get()
        
        index_status = 'index'
        follow_status = 'follow'
        
        if robots_meta:
            if 'noindex' in robots_meta.lower():
                index_status = 'noindex'
            if 'nofollow' in robots_meta.lower():
                follow_status = 'nofollow'

        page_info = {
            'type': 'page_info',
            'url': self.url,
            'http_status_code': http_status_code,
            'index_status': index_status,
            'follow_status': follow_status,
            'title': response.xpath('//title/text()').get('').strip(),
            'meta_description': response.xpath("//meta[@name='description']/@content").get('').strip()
        }
        self.scraped_data.append(page_info)
        self.logger.info(f"Added page info: {page_info}")

        soup = BeautifulSoup(response.text, 'html.parser')
        anchor_tags = soup.find_all('a', href=True)
        
        internal_links = []
        external_links = []
        
        for a_tag in anchor_tags:
            href = a_tag.get('href', '').strip()
            if not href:
                continue
                
            if not href.startswith(('http://', 'https://')):
                href = urljoin(self.url, href)
            
            href = href.rstrip('/')
            
            rel = a_tag.get('rel', '')
            if isinstance(rel, list):
                rel = ' '.join(rel)
            
            link_type = 'nofollow' if rel and 'nofollow' in rel.lower() else 'follow'
            anchor_text = a_tag.get_text(strip=True)
            
            link_data = {
                'type': 'link',
                'url': href,
                'anchor_text': anchor_text,
                'link_type': link_type,
                'target': a_tag.get('target', ''),
            }
            
            link_domain = urlparse(href).netloc
            if link_domain == self.base_domain:
                link_data['link_category'] = 'internal'
                internal_links.append(link_data)
            else:
                link_data['link_category'] = 'external'
                external_links.append(link_data)
            
            self.scraped_data.append(link_data)
            self.logger.info(f"Added link: {link_data}")
        
        summary = {
            'type': 'summary',
            'total_links': len(anchor_tags),
            'internal_links_count': len(internal_links),
            'external_links_count': len(external_links),
            'follow_links': len([l for l in self.scraped_data if l.get('link_type') == 'follow']),
            'nofollow_links': len([l for l in self.scraped_data if l.get('link_type') == 'nofollow'])
        }
        self.scraped_data.append(summary)
        self.logger.info(f"Added summary: {summary}")

    def closed(self, reason):
        self.logger.info(f"Spider closed. Scraped data: {len(self.scraped_data)} items")
        return self.scraped_data

class ContactScraper(Spider):
    name = "contact_scraper"

    def __init__(self, url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not url:
            raise ValueError("Please provide url")

        self.url = url.rstrip('/')
        self.base_domain = urlparse(url).netloc
        self.allowed_domains = [self.base_domain]
        self.scraped_data = []  # Store scraped data here
        self.visited_urls = set([self.url])

        self.contact_keywords = [
            'contact', 'contactus', 'contact-us', 'about', 'aboutus', 'about-us',
            'get-in-touch', 'connect', 'support', 'help', 'info', 'reach-us',
            'getintouch', 'contact-form', 'reach-out', 'customer-service'
        ]

        self.logger.info(f"Initialized ContactScraper for {url}")

    def start_requests(self):
        self.logger.info(f"Requesting URL: {self.url}")
        yield scrapy.Request(
            self.url,
            callback=self.parse_initial,
            errback=self.handle_error,
        )

    def parse_initial(self, response):
        current_url = response.url.rstrip('/')
        self.logger.info(f"Parsing initial URL: {current_url}")

        # Process the initial page
        yield from self.parse_page(response)

        # Find and follow contact pages
        soup = BeautifulSoup(response.text, 'html.parser')
        contact_urls = self.find_contact_pages(soup, current_url)
        self.logger.info(f"Found {len(contact_urls)} potential contact pages: {contact_urls}")

        for contact_url in contact_urls:
            if contact_url not in self.visited_urls:
                self.visited_urls.add(contact_url)
                self.logger.info(f"Following contact page: {contact_url}")
                yield scrapy.Request(
                    contact_url,
                    callback=self.parse_page,
                    errback=self.handle_error
                )

    def parse_page(self, response):
        current_url = response.url.rstrip('/')
        self.logger.info(f"Parsing page: {current_url}")

        if response.status != 200:
            self.logger.warning(f"Non-200 status code {response.status} for {current_url}")
            contact_data = {
                'type': 'contact_info',
                'url': self.url,
                'emails': [],
                'phone_numbers': [],
                'found_on_page': current_url,
                'status': 'error',
                'error_message': f"Non-200 status code: {response.status}"
            }
            self.scraped_data.append(contact_data)
            return contact_data

        soup = BeautifulSoup(response.text, 'html.parser')
        emails = self.extract_emails(soup)
        phones = self.extract_phone_numbers(soup)

        contact_data = {
            'type': 'contact_info',
            'url': self.url,
            'emails': list(set(emails)),
            'phone_numbers': list(set(phones)),
            'found_on_page': current_url,
            'status': 'found' if emails or phones else 'not_found'
        }

        self.scraped_data.append(contact_data)
        self.logger.info(f"Appended contact data for {current_url}: {contact_data}")
        return contact_data

    def extract_emails(self, soup):
        text = soup.get_text()
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        emails = re.findall(email_pattern, text)
        return [email.lower() for email in emails if re.match(email_pattern, email)]

    def extract_phone_numbers(self, soup):
        text = soup.get_text()
        phone_numbers = []
        try:
            for match in phonenumbers.PhoneNumberMatcher(text, None):
                phone = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
                phone_numbers.append(phone)
        except Exception as e:
            self.logger.warning(f"Error extracting phone numbers: {e}")
        return phone_numbers

    def find_contact_pages(self, soup, current_url):
        contact_urls = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '').strip()
            if not href:
                continue
            if not href.startswith(('http://', 'https://')):
                href = urljoin(current_url, href)
            href = href.rstrip('/')
            if any(keyword in href.lower() for keyword in self.contact_keywords):
                if urlparse(href).netloc == self.base_domain:
                    contact_urls.append(href)
        return list(set(contact_urls))

    def handle_error(self, failure):
        self.logger.error(f"Request failed for {failure.request.url}: {str(failure)}")
        contact_data = {
            'type': 'contact_info',
            'url': self.url,
            'emails': [],
            'phone_numbers': [],
            'found_on_page': failure.request.url,
            'status': 'error',
            'error_message': str(failure)
        }
        self.scraped_data.append(contact_data)
        return contact_data

    def closed(self, reason):
        self.logger.info(f"Spider closed. Scraped data: {len(self.scraped_data)} items")
        return self.scraped_data