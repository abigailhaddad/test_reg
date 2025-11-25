import requests
from bs4 import BeautifulSoup
import json
import time
from urllib.parse import urljoin, urlparse
from collections import deque
import os
import hashlib
from pathlib import Path
import re

class NYRegulationsScraper:
    def __init__(self, cache_dir='scraped_data'):
        self.base_url = "https://www.law.cornell.edu/regulations/new-york"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.visited_urls = set()
        self.all_regulations = []
        self.failed_urls = set()
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.progress_file = self.cache_dir / 'progress.json'
        self.failed_file = self.cache_dir / 'failed_urls.json'
        self.load_progress()
    
    def load_progress(self):
        """Load previously visited URLs and failed URLs"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    self.visited_urls = set(data.get('visited_urls', []))
                    print(f"Loaded {len(self.visited_urls)} previously visited URLs")
            except Exception as e:
                print(f"Error loading progress: {e}")
        
        if self.failed_file.exists():
            try:
                with open(self.failed_file, 'r') as f:
                    self.failed_urls = set(json.load(f))
                    print(f"Loaded {len(self.failed_urls)} previously failed URLs")
            except Exception as e:
                print(f"Error loading failed URLs: {e}")
    
    def save_progress(self):
        """Save current progress"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump({
                    'visited_urls': list(self.visited_urls),
                    'total_scraped': len(self.all_regulations)
                }, f, indent=2)
            
            if self.failed_urls:
                with open(self.failed_file, 'w') as f:
                    json.dump(list(self.failed_urls), f, indent=2)
        except Exception as e:
            print(f"Error saving progress: {e}")
    
    def get_cache_path(self, url):
        """Generate cache file path for a URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return self.cache_dir / f"{url_hash}.json"
    
    def get_page(self, url, max_retries=3):
        """Get page with retry logic and caching"""
        cache_path = self.get_cache_path(url)
        
        # Check cache first
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                    return cached_data.get('html')
            except Exception:
                pass  # If cache is corrupted, re-fetch
        
        # Try to fetch with retries
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                html = response.text
                
                # Cache the result
                try:
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump({'url': url, 'html': html}, f, ensure_ascii=False)
                except Exception as e:
                    print(f"Warning: Could not cache {url}: {e}")
                
                return html
                
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"All attempts failed for {url}")
                    self.failed_urls.add(url)
                    return None
    
    def parse_main_page(self):
        html = self.get_page(self.base_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        title_links = []
        
        # Find all title links (main categories)
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href and '/regulations/new-york/title-' in href:
                full_url = urljoin(self.base_url, href)
                title = link.get_text(strip=True)
                if title and full_url not in self.visited_urls:
                    title_links.append({
                        'title': title,
                        'url': full_url,
                        'type': 'title'
                    })
        
        return title_links
    
    def find_regulation_links(self, soup, base_url):
        links = []
        
        # Look for various link patterns in NY regulations
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if not href:
                continue
                
            # Skip non-regulation links
            if any(skip in href for skip in ['#', 'mailto:', 'javascript:', 'http://www.', 'https://www.']):
                if not 'cornell.edu' in href:
                    continue
            
            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)
            
            # Only process Cornell NY regulation URLs
            if 'law.cornell.edu/regulations/new-york' not in full_url:
                continue
                
            # Skip if already visited
            if full_url in self.visited_urls:
                continue
                
            link_text = link.get_text(strip=True)
            if not link_text:
                continue
                
            # Determine link type based on URL pattern
            link_type = 'unknown'
            if '/title-' in href:
                link_type = 'title'
            elif '-NYCRR-' in href:
                link_type = 'regulation'
            elif '/chapter-' in href or '/part-' in href or '/section-' in href:
                link_type = 'subsection'
            elif '/app-' in href or '/appendix' in href:
                link_type = 'appendix'
                
            links.append({
                'title': link_text,
                'url': full_url,
                'type': link_type
            })
        
        return links
    
    def scrape_regulation_content(self, url):
        html = self.get_page(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract title
        title_elem = soup.find('h1') or soup.find('title')
        title = title_elem.get_text(strip=True) if title_elem else "No title found"
        
        # Extract main content - be more selective about content areas
        content = ""
        
        # Look for main content containers
        main_content = soup.find('div', {'id': 'content'}) or soup.find('main') or soup.find('article')
        
        if main_content:
            # Remove navigation, ads, and other non-content elements
            for unwanted in main_content(['nav', 'aside', 'footer', 'script', 'style', 'noscript']):
                unwanted.decompose()
            
            content = main_content.get_text(separator='\n', strip=True)
        else:
            # Fallback: get all text but filter out obvious navigation
            for script in soup(["script", "style", "nav", "footer"]):
                script.decompose()
            content = soup.get_text(separator='\n', strip=True)
        
        # Clean the content for better readability
        cleaned_content = self.clean_regulation_text(content)
        
        return {
            'url': url,
            'title': title,
            'content': content,
            'cleaned_content': cleaned_content,
            'url_type': self.classify_url(url)
        }
    
    def clean_regulation_text(self, content):
        """Clean and format regulation text for better readability"""
        if not content:
            return ""
        
        # Remove excessive whitespace
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        
        # Remove common navigation/UI text
        lines = content.split('\n')
        cleaned_lines = []
        
        skip_patterns = [
            r'^\s*Menu\s*$',
            r'^\s*Search\s*$', 
            r'^\s*Home\s*$',
            r'^\s*Back to top\s*$',
            r'^\s*Print\s*$',
            r'^\s*Share\s*$',
            r'^\s*LII\s*$',
            r'^\s*Legal Information Institute\s*$',
            r'^\s*Cornell Law School\s*$',
            r'^\s*Compare\s*$',
            r'^\s*Table of Contents\s*$',
            r'^\s*›\s*$',
            r'^\s*»\s*$',
            r'^\s*\|\s*$',
            r'^\s*Related\s*$',
            r'^\s*Previous\s*$',
            r'^\s*Next\s*$',
            r'^\s*Toggle navigation.*$',
            r'^\s*Skip to main content.*$'
        ]
        
        for line in lines:
            # Skip empty lines
            if not line.strip():
                continue
                
            # Skip navigation patterns
            skip_line = False
            for pattern in skip_patterns:
                if re.match(pattern, line, re.IGNORECASE):
                    skip_line = True
                    break
            
            if not skip_line:
                cleaned_lines.append(line.strip())
        
        # Join lines with proper spacing
        cleaned_content = '\n'.join(cleaned_lines)
        
        # Fix all types of newline issues
        # Handle literal \n sequences that appear as text
        cleaned_content = re.sub(r'\\n', '\n', cleaned_content)
        
        # Handle patterns like "7802)\nChapter" -> "7802)\n\nChapter"
        cleaned_content = re.sub(r'([0-9)])\n([A-Z][a-z])', r'\1\n\n\2', cleaned_content)
        cleaned_content = re.sub(r'([a-z])\n([A-Z][a-z])', r'\1\n\n\2', cleaned_content)
        
        # Clean up section numbering and formatting
        cleaned_content = re.sub(r'^(\d+\.\d+\.\d+)\s+', r'\1. ', cleaned_content, flags=re.MULTILINE)
        cleaned_content = re.sub(r'^(\([a-z]\))\s+', r'\1 ', cleaned_content, flags=re.MULTILINE)
        
        # Remove excessive spacing around punctuation
        cleaned_content = re.sub(r'\s+([.,;:])', r'\1', cleaned_content)
        cleaned_content = re.sub(r'([.,;:])\s+', r'\1 ', cleaned_content)
        
        # Final cleanup of excessive newlines
        cleaned_content = re.sub(r'\n\s*\n\s*\n+', '\n\n', cleaned_content)
        
        return cleaned_content.strip()
    
    def classify_url(self, url):
        if '/title-' in url:
            return 'title'
        elif '-NYCRR-' in url:
            return 'regulation'
        elif '/chapter-' in url:
            return 'chapter'
        elif '/part-' in url:
            return 'part'
        elif '/section-' in url:
            return 'section'
        elif '/app-' in url or '/appendix' in url:
            return 'appendix'
        else:
            return 'unknown'
    
    def crawl_recursively(self, start_urls, max_pages=None):
        to_visit = deque(start_urls)
        scraped_count = len(self.all_regulations)  # Start from existing count
        save_interval = 50  # Save progress every 50 pages
        
        while to_visit and (max_pages is None or scraped_count < max_pages):
            current_url = to_visit.popleft()
            
            # Skip if already processed or failed
            if current_url in self.visited_urls or current_url in self.failed_urls:
                continue
            
            print(f"Scraping {scraped_count + 1}: {current_url}")
            self.visited_urls.add(current_url)
            
            # Scrape current page content
            content_data = self.scrape_regulation_content(current_url)
            if content_data:
                self.all_regulations.append(content_data)
                scraped_count += 1
            
            # Get page HTML to find more links
            html = self.get_page(current_url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                new_links = self.find_regulation_links(soup, current_url)
                
                # Add new links to queue
                for link_info in new_links:
                    if (link_info['url'] not in self.visited_urls and 
                        link_info['url'] not in self.failed_urls):
                        to_visit.append(link_info['url'])
            
            # Save progress periodically
            if scraped_count % save_interval == 0:
                self.save_progress()
                print(f"Progress saved. Queue size: {len(to_visit)}")
            
            # Be respectful to the server
            time.sleep(0.5)
        
        # Final save
        self.save_progress()
        return self.all_regulations
    
    def retry_failed_urls(self, max_retries=2):
        """Retry previously failed URLs"""
        if not self.failed_urls:
            print("No failed URLs to retry")
            return
        
        print(f"Retrying {len(self.failed_urls)} failed URLs...")
        failed_copy = self.failed_urls.copy()
        self.failed_urls.clear()
        
        for url in failed_copy:
            print(f"Retrying: {url}")
            content_data = self.scrape_regulation_content(url)
            if content_data:
                self.all_regulations.append(content_data)
                self.visited_urls.add(url)
                print(f"✅ Retry successful: {url}")
            else:
                self.failed_urls.add(url)
            time.sleep(1)  # Be extra careful on retries
    
    def scrape_all(self, max_pages=None):
        print("Starting comprehensive scrape of NY regulations...")
        
        # Load any existing scraped data
        existing_files = list(self.cache_dir.glob('*.json'))
        if existing_files:
            print(f"Found {len(existing_files)} cached files")
        
        # Start with main title pages if we don't have visited URLs already
        if not self.visited_urls:
            title_links = self.parse_main_page()
            if not title_links:
                print("No title links found on main page")
                return []
            print(f"Found {len(title_links)} main titles to crawl")
            start_urls = [link['url'] for link in title_links]
        else:
            print(f"Resuming from {len(self.visited_urls)} previously visited URLs")
            # Load existing regulation data from cache
            for cache_file in self.cache_dir.glob('*.json'):
                if cache_file.name in ['progress.json', 'failed_urls.json']:
                    continue
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_data = json.load(f)
                        if 'url' in cached_data and 'html' in cached_data:
                            # This is a cached HTML file, convert to regulation data
                            content_data = self.scrape_regulation_content(cached_data['url'])
                            if content_data:
                                self.all_regulations.append(content_data)
                except Exception as e:
                    print(f"Error loading cached data from {cache_file}: {e}")
            
            start_urls = []  # Will be populated from cached links
        
        # Start recursive crawling
        all_data = self.crawl_recursively(start_urls, max_pages)
        
        # Try failed URLs one more time
        if self.failed_urls:
            print(f"\nRetrying {len(self.failed_urls)} failed URLs...")
            self.retry_failed_urls()
        
        return all_data
    
    def save_to_json(self, data, filename='ny_regulations.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {filename}")

if __name__ == "__main__":
    scraper = NYRegulationsScraper()
    
    # For testing, limit to 50 pages. Remove max_pages parameter to scrape everything
    data = scraper.scrape_all(max_pages=50)
    
    if data:
        scraper.save_to_json(data)
        print(f"Successfully scraped {len(data)} regulations")
        
        # Print summary of what we found
        types_found = {}
        for item in data:
            url_type = item.get('url_type', 'unknown')
            types_found[url_type] = types_found.get(url_type, 0) + 1
        
        print("\nTypes of content found:")
        for content_type, count in types_found.items():
            print(f"  {content_type}: {count}")
    else:
        print("No data scraped")