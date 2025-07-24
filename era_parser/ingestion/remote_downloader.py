import os
import json
import requests
import tempfile
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse
import time
import concurrent.futures
import xml.etree.ElementTree as ET
import re

class RemoteEraDownloader:
    """Optimized downloads and processes era files from remote URLs with fast discovery"""
    
    def __init__(self, base_url: str, network: str, download_dir: Optional[str] = None, 
                 cleanup: bool = True, max_retries: int = 3):
        """
        Initialize remote era downloader
        
        Args:
            base_url: Base URL for era files
            network: Network name (gnosis, mainnet, sepolia)
            download_dir: Directory for temporary downloads (None = system temp)
            cleanup: Whether to delete files after processing
            max_retries: Maximum retry attempts for downloads
        """
        self.base_url = base_url.rstrip('/')
        self.network = network.lower()
        self.cleanup = cleanup
        self.max_retries = max_retries
        
        # Parse URL to determine if it's S3
        parsed_url = urlparse(self.base_url)
        self.is_s3 = 's3' in parsed_url.hostname if parsed_url.hostname else False
        
        # Setup download directory
        if download_dir:
            self.download_dir = Path(download_dir)
            self.download_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.download_dir = Path(tempfile.gettempdir()) / "era_downloads"
            self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Progress tracking
        self.progress_file = self.download_dir / f".era_progress_{network}.json"
        self.progress_data = self._load_progress()
        
        print(f"🌐 Optimized Remote Era Downloader initialized")
        print(f"   Base URL: {self.base_url}")
        print(f"   Network: {self.network}")
        print(f"   S3 Detected: {self.is_s3}")
        print(f"   Download dir: {self.download_dir}")
        print(f"   Cleanup after processing: {self.cleanup}")
    
    def _load_progress(self) -> Dict[str, Any]:
        """Load progress from previous runs"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            "network": self.network,
            "processed_eras": [],
            "failed_eras": [],
            "last_run": None
        }
    
    def _save_progress(self):
        """Save current progress"""
        self.progress_data["last_run"] = time.time()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress_data, f, indent=2)
    
    def discover_era_files(self, start_era: int, end_era: Optional[int] = None) -> List[Tuple[int, str]]:
        """
        OPTIMIZED: Fast discovery of available era files using bulk S3 listing or parallel requests
        
        Args:
            start_era: Starting era number
            end_era: Ending era number (None = discover until not found)
            
        Returns:
            List of (era_number, url) tuples
        """
        print(f"🚀 Fast discovery starting from era {start_era}")
        
        if self.is_s3:
            # OPTIMIZATION 1: Use S3 bulk listing for much faster discovery
            return self._discover_s3_bulk(start_era, end_era)
        else:
            # OPTIMIZATION 2: Use parallel requests for non-S3 URLs
            return self._discover_parallel(start_era, end_era)
    
    def _discover_s3_bulk(self, start_era: int, end_era: Optional[int] = None) -> List[Tuple[int, str]]:
        """
        OPTIMIZATION: Bulk S3 listing with proper pagination to get ALL era files
        """
        print(f"📦 Using S3 bulk listing for ultra-fast discovery")
        
        all_available_eras = []
        continuation_token = None
        
        try:
            session = requests.Session()
            session.headers.update({'User-Agent': 'era-parser/1.0'})
            
            # Paginate through ALL era files using S3 continuation tokens
            page = 1
            while True:
                # Build S3 list request with prefix for the network
                list_url = f"{self.base_url}/?list-type=2&prefix={self.network}-&max-keys=1000"
                
                # Add continuation token for pagination (with proper URL encoding)
                if continuation_token:
                    import urllib.parse
                    encoded_token = urllib.parse.quote(continuation_token, safe='')
                    list_url += f"&continuation-token={encoded_token}"
                
                print(f"   🔍 Fetching S3 bucket listing (page {page})...")
                response = session.get(list_url, timeout=30)
                
                if response.status_code == 200:
                    page_eras = self._parse_s3_listing(response.text, start_era, end_era)
                    all_available_eras.extend(page_eras)
                    
                    print(f"   📊 Page {page}: Found {len(page_eras)} era files")
                    
                    # Check if there are more results
                    continuation_token = self._extract_continuation_token(response.text)
                    if not continuation_token:
                        break  # No more pages
                    
                    page += 1
                    
                    # Safety check to prevent infinite loops (increased limit for large buckets)
                    if page > 500:  # Max 500,000 files (500 pages * 1000 each)
                        print(f"   ⚠️  Reached maximum page limit, stopping pagination")
                        break
                        
                else:
                    print(f"   ⚠️  S3 listing page {page} failed (status {response.status_code})")
                    if page == 1:
                        # If first page fails, fall back to parallel
                        print(f"   ⚠️  First page failed, falling back to parallel discovery")
                        return self._discover_parallel(start_era, end_era)
                    else:
                        # If later page fails, return what we have so far
                        print(f"   ⚠️  Pagination failed, returning {len(all_available_eras)} files found so far")
                        break
            
            print(f"   🎯 Total found: {len(all_available_eras)} era files across {page} pages")
            return all_available_eras
                
        except Exception as e:
            print(f"   ⚠️  S3 bulk listing failed: {e}, falling back to parallel discovery")
            return self._discover_parallel(start_era, end_era)
    
    def _extract_continuation_token(self, xml_content: str) -> Optional[str]:
        """
        Extract NextContinuationToken from S3 XML response
        """
        try:
            # Try XML parsing first
            root = ET.fromstring(xml_content)
            
            # Look for NextContinuationToken with different namespace approaches
            for ns_prefix in ['', 's3:']:
                ns_dict = {'': 'http://s3.amazonaws.com/doc/2006-03-01/', 's3': 'http://s3.amazonaws.com/doc/2006-03-01/'} if ns_prefix else {}
                token_elem = root.find(f'.//{ns_prefix}NextContinuationToken', ns_dict)
                if token_elem is not None and token_elem.text:
                    return token_elem.text
            
            # Try without namespace
            token_elem = root.find('.//NextContinuationToken')
            if token_elem is not None and token_elem.text:
                return token_elem.text
                
        except ET.ParseError:
            # If XML parsing fails, use regex
            import re
            match = re.search(r'<NextContinuationToken>([^<]+)</NextContinuationToken>', xml_content)
            if match:
                return match.group(1)
        
        return None
    
    def _parse_s3_listing(self, xml_content: str, start_era: int, end_era: Optional[int] = None) -> List[Tuple[int, str]]:
        """
        Parse S3 XML listing to extract era file URLs
        """
        available_eras = []
        
        try:
            # Try XML parsing first
            root = ET.fromstring(xml_content)
            
            # Handle different XML namespaces
            namespaces = {
                '': 'http://s3.amazonaws.com/doc/2006-03-01/',
                's3': 'http://s3.amazonaws.com/doc/2006-03-01/'
            }
            
            # Find all Key elements
            keys = []
            for ns_prefix in ['', 's3:']:
                ns_dict = namespaces if ns_prefix else {}
                contents = root.findall(f'.//{ns_prefix}Contents', ns_dict)
                for content_elem in contents:
                    key_elem = content_elem.find(f'{ns_prefix}Key', ns_dict)
                    if key_elem is not None and key_elem.text:
                        keys.append(key_elem.text)
            
            # Also try without namespace
            if not keys:
                for content_elem in root.findall('.//Contents'):
                    key_elem = content_elem.find('Key')
                    if key_elem is not None and key_elem.text:
                        keys.append(key_elem.text)
                        
        except ET.ParseError:
            # If XML parsing fails, use regex on raw content
            print(f"   📝 XML parsing failed, using regex extraction")
            keys = self._extract_keys_with_regex(xml_content)
        
        # Process found keys and filter by era range
        era_pattern = rf'{self.network}-(\d{{5}})-[a-f0-9]{{8}}\.era'
        
        for key in keys:
            match = re.match(era_pattern, key)
            if match:
                era_number = int(match.group(1))
                
                # Apply era range filters
                if era_number < start_era:
                    continue
                if end_era is not None and era_number > end_era:
                    continue
                
                url = f"{self.base_url}/{key}"
                available_eras.append((era_number, url))
        
        # Sort by era number
        available_eras.sort(key=lambda x: x[0])
        
        return available_eras
    
    def _extract_keys_with_regex(self, content: str) -> List[str]:
        """
        Extract S3 keys using regex when XML parsing fails
        """
        keys = []
        
        # Try different patterns for S3 listing formats
        patterns = [
            rf'<Key>({self.network}-\d{{5}}-[a-f0-9]{{8}}\.era)</Key>',
            rf'>({self.network}-\d{{5}}-[a-f0-9]{{8}}\.era)<',
            rf'{self.network}-\d{{5}}-[a-f0-9]{{8}}\.era'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                keys.extend(matches)
                break
        
        return list(set(keys))  # Remove duplicates
    
    def _discover_parallel(self, start_era: int, end_era: Optional[int] = None) -> List[Tuple[int, str]]:
        """
        OPTIMIZATION: Parallel discovery for non-S3 URLs or S3 fallback - finds ALL era files
        """
        print(f"⚡ Using parallel discovery")
        
        available_eras = []
        
        # For open-ended ranges, we need to discover the actual range first
        if end_era is None:
            print(f"   🔍 Open-ended range detected, discovering actual range...")
            # Start with a reasonable estimate and expand as needed
            estimated_end = self._estimate_max_era(start_era)
            print(f"   📊 Estimated range: {start_era} to {estimated_end}")
            era_range = list(range(start_era, estimated_end + 1))
        else:
            era_range = list(range(start_era, end_era + 1))
        
        print(f"   📋 Checking {len(era_range)} eras in total")
        
        # OPTIMIZATION: Process in parallel batches
        batch_size = 100  # Increased batch size for better throughput
        
        for batch_start in range(0, len(era_range), batch_size):
            batch_eras = era_range[batch_start:batch_start + batch_size]
            
            print(f"   🔍 Checking eras {batch_eras[0]}-{batch_eras[-1]} ({len(batch_eras)} in parallel)")
            
            batch_results = self._check_eras_parallel(batch_eras)
            found_in_batch = len(batch_results)
            available_eras.extend(batch_results)
            
            print(f"   📊 Batch result: {found_in_batch}/{len(batch_eras)} found")
            
            # For open-ended ranges, continue until we hit significant gaps
            if end_era is None:
                # If we find very few files in recent batches, we might be near the end
                if found_in_batch < 5 and batch_eras[0] > start_era + 1000:
                    # Check if we've hit a significant gap
                    consecutive_empty_batches = self._count_consecutive_empty_batches(available_eras, batch_eras[0], batch_size)
                    if consecutive_empty_batches >= 3:  # 3 mostly empty batches = likely at the end
                        print(f"   🛑 Found {consecutive_empty_batches} consecutive mostly-empty batches, likely reached end")
                        break
        
        available_eras.sort(key=lambda x: x[0])
        print(f"   🎯 Parallel discovery complete: {len(available_eras)} era files found")
        return available_eras
    
    def _estimate_max_era(self, start_era: int) -> int:
        """
        Estimate the maximum era number by checking a few high values
        """
        # For Gnosis, we know there are 2600+ eras, so let's start with a reasonable estimate
        test_eras = [1000, 2000, 3000, 4000, 5000]
        
        max_found = start_era
        
        print(f"   🎯 Quick estimation check...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_era = {executor.submit(self._check_single_era, era): era for era in test_eras}
            
            for future in concurrent.futures.as_completed(future_to_era, timeout=30):
                era = future_to_era[future]
                try:
                    result = future.result()
                    if result:  # Era exists
                        max_found = max(max_found, era)
                        print(f"   ✅ Era {era} exists")
                    else:
                        print(f"   ❌ Era {era} not found")
                except Exception:
                    print(f"   ❌ Era {era} check failed")
        
        # Add buffer above the highest found era
        estimated_max = max_found + 1000
        print(f"   📊 Highest confirmed era: {max_found}, estimating max: {estimated_max}")
        return estimated_max
    
    def _count_consecutive_empty_batches(self, available_eras: List[Tuple[int, str]], current_era: int, batch_size: int) -> int:
        """
        Count how many recent batches had very few results
        """
        if not available_eras:
            return 0
        
        # Look at the last few batches worth of eras
        recent_eras = [era for era, _ in available_eras if era >= current_era - (batch_size * 3)]
        batches_checked = (current_era - (current_era - len(recent_eras))) // batch_size
        
        if batches_checked == 0:
            return 0
        
        avg_per_batch = len(recent_eras) / max(1, batches_checked)
        
        # If average is very low, consider it "empty"
        return 3 if avg_per_batch < 5 else 0
    
    def _check_eras_parallel(self, era_numbers: List[int]) -> List[Tuple[int, str]]:
        """
        Check multiple eras in parallel using ThreadPoolExecutor
        """
        available_eras = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all era checks
            future_to_era = {
                executor.submit(self._check_single_era, era_num): era_num 
                for era_num in era_numbers
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_era, timeout=60):
                era_num = future_to_era[future]
                try:
                    result = future.result()
                    if result:
                        available_eras.append((era_num, result))
                except Exception as e:
                    print(f"   ❌ Era {era_num} check failed: {e}")
        
        return available_eras
    
    def _check_single_era(self, era_number: int) -> Optional[str]:
        """
        Check if a single era exists and return its URL
        """
        try:
            # Build the URL pattern for this era
            era_str = f"{era_number:05d}"
            
            # Method 1: Try pattern matching for S3-like URLs
            if self.is_s3:
                # For S3, try a HEAD request with common patterns
                common_patterns = [
                    f"{self.base_url}/{self.network}-{era_str}.era",  # Without hash
                ]
                
                for url in common_patterns:
                    if self._url_exists(url):
                        return url
            
            # Method 2: Try directory listing approach
            return self._discover_era_file_with_hash_fast(era_number)
            
        except Exception as e:
            return None
    
    def _url_exists(self, url: str, timeout: int = 5) -> bool:
        """
        Fast check if URL exists using HEAD request
        """
        try:
            response = requests.head(url, timeout=timeout, allow_redirects=True)
            return response.status_code == 200
        except:
            return False
    
    def _discover_era_file_with_hash_fast(self, era_number: int) -> Optional[str]:
        """
        Fast discovery of era file with hash - optimized version
        """
        era_str = f"{era_number:05d}"
        
        try:
            # Quick S3 prefix listing for this specific era
            if self.is_s3:
                list_url = f"{self.base_url}/?list-type=2&prefix={self.network}-{era_str}-&max-keys=5"
                response = requests.get(list_url, timeout=10)
                
                if response.status_code == 200:
                    # Quick regex search for the file
                    pattern = rf'{self.network}-{era_str}-[a-f0-9]{{8}}\.era'
                    matches = re.findall(pattern, response.text, re.IGNORECASE)
                    if matches:
                        return f"{self.base_url}/{matches[0]}"
            
            return None
            
        except Exception:
            return None
    
    def _download_file(self, url: str, local_path: Path) -> bool:
        """Download a file with retry logic"""
        for attempt in range(self.max_retries):
            try:
                print(f"   📥 Downloading (attempt {attempt + 1}/{self.max_retries})")
                
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                # Get file size if available
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Simple progress indicator
                            if total_size > 0 and downloaded % (1024 * 1024) == 0:  # Every MB
                                progress = (downloaded / total_size) * 100
                                print(f"   📊 Progress: {progress:.1f}% ({downloaded // (1024*1024)}MB / {total_size // (1024*1024)}MB)", end='\r')
                
                if total_size > 0:
                    print(f"   ✅ Downloaded: {total_size // (1024*1024)}MB")
                else:
                    print(f"   ✅ Downloaded: {local_path.stat().st_size // (1024*1024)}MB")
                    
                return True
                
            except Exception as e:
                print(f"   ❌ Download attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"   ❌ All download attempts failed")
                    return False
        
        return False
    
    def download_era(self, era_number: int, url: str) -> Optional[str]:
        """
        Download a specific era file
        
        Args:
            era_number: Era number
            url: Direct URL to era file
            
        Returns:
            Local file path if successful, None if failed
        """
        era_str = f"{era_number:05d}"
        filename = f"{self.network}-{era_str}.era"
        local_path = self.download_dir / filename
        
        # Skip if already exists and is valid
        if local_path.exists() and local_path.stat().st_size > 0:
            print(f"   ♻️  Era {era_number} already downloaded: {local_path}")
            return str(local_path)
        
        print(f"📥 Downloading era {era_number}")
        print(f"   URL: {url}")
        print(f"   Local: {local_path}")
        
        if self._download_file(url, local_path):
            return str(local_path)
        else:
            # Clean up partial download
            if local_path.exists():
                local_path.unlink()
            return None
    
    def cleanup_era(self, local_path: str):
        """Delete local era file after processing"""
        if self.cleanup:
            try:
                Path(local_path).unlink()
                print(f"   🗑️  Cleaned up: {local_path}")
            except Exception as e:
                print(f"   ⚠️  Cleanup failed: {e}")

    def get_processed_eras_from_state(self, network: str, start_era: int = None, end_era: int = None) -> set:
        """
        Get list of fully processed era numbers from era state manager.
        
        Args:
            network: Network name
            start_era: Start era filter
            end_era: End era filter
            
        Returns:
            Set of era numbers that are fully processed
        """
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            
            # Query for fully completed eras in range using the era_processing_progress view
            import clickhouse_connect
            client = clickhouse_connect.get_client(
                host=state_manager.host,
                port=state_manager.port,
                username=state_manager.user,
                password=state_manager.password,
                database=state_manager.database,
                secure=state_manager.secure,
                verify=False
            )
            
            # Build query with range filters using the progress view
            query = f"""
            SELECT era_number
            FROM {state_manager.database}.era_processing_progress
            WHERE network = '{network}'
              AND completed_datasets = total_datasets
              AND completed_datasets > 0
            """
            
            if start_era is not None:
                query += f" AND era_number >= {start_era}"
            if end_era is not None:
                query += f" AND era_number <= {end_era}"
                
            query += " ORDER BY era_number"
            
            result = client.query(query)
            processed_eras = {row[0] for row in result.result_rows}
            
            print(f"📋 Found {len(processed_eras)} fully processed eras in era state for {network}")
            
            return processed_eras
            
        except Exception as e:
            print(f"⚠️  Could not check era state: {e}")
            return set()
    
    def process_era_range(self, start_era: int, end_era: Optional[int], 
                     command: str, base_output: str, separate_files: bool = False,
                     resume: bool = False, export_type: str = "file",
                     processed_eras: set = None) -> Dict[str, Any]:
        """
        Download and process a range of era files with enhanced state management
        
        Args:
            start_era: Starting era number
            end_era: Ending era number (None = until not found)
            command: Processing command (all-blocks, transactions, etc.)
            base_output: Base output filename
            separate_files: Whether to create separate files per data type
            resume: Whether to skip already processed eras
            export_type: "file" or "clickhouse"
            processed_eras: Set of already processed eras (for ClickHouse)
            
        Returns:
            Processing summary
        """
        print(f"🚀 Starting remote era processing")
        print(f"   Range: {start_era} to {end_era or 'end'}")
        print(f"   Command: {command}")
        print(f"   Output: {base_output}")
        print(f"   Separate files: {separate_files}")
        print(f"   Resume: {resume}")
        print(f"   Export type: {export_type}")
        
        # OPTIMIZED: Fast discovery of available eras
        available_eras = self.discover_era_files(start_era, end_era)
        
        if not available_eras:
            print("❌ No era files found in the specified range")
            return {"success": False, "processed_count": 0, "failed_count": 0}
        
        # Filter out already processed eras if resuming
        if resume:
            processed_eras_file = set(self.progress_data.get("processed_eras", []))
            available_eras = [(era, url) for era, url in available_eras if era not in processed_eras_file]
            print(f"📋 Resume mode: {len(available_eras)} eras remaining after filtering file processed ones")
        
        # Filter out ClickHouse processed eras using era state manager
        if export_type == "clickhouse":
            try:
                state_processed_eras = self.get_processed_eras_from_state(self.network, start_era, end_era)
                available_eras = [(era, url) for era, url in available_eras if era not in state_processed_eras]
                print(f"📋 Era state filter: {len(available_eras)} eras remaining after filtering state processed ones")
            except Exception as e:
                print(f"⚠️  Could not filter by era state: {e}")
        elif processed_eras:  # Legacy filter for other export types
            available_eras = [(era, url) for era, url in available_eras if era not in processed_eras]
            print(f"📋 Legacy filter: {len(available_eras)} eras remaining")
        
        # Process each era
        processed_count = 0
        failed_count = 0
        failed_eras = []
        
        # Import here to avoid circular dependencies
        from ..core import EraProcessor
        
        for i, (era_number, url) in enumerate(available_eras, 1):
            print(f"\n{'='*60}")
            print(f"📈 Processing era {era_number} ({i}/{len(available_eras)})")
            print(f"{'='*60}")
            
            try:
                # Download the era file
                local_path = self.download_era(era_number, url)
                if not local_path:
                    failed_count += 1
                    failed_eras.append(era_number)
                    continue
                
                # Process using EraProcessor
                processor = EraProcessor()
                processor.setup(local_path)
                
                # Generate output filename
                if export_type == "file":
                    output_file = self._generate_era_output_filename(base_output, era_number)
                    print(f"   📂 Output: {output_file}")
                else:
                    output_file = "clickhouse_output"  # Not used for ClickHouse
                    print(f"   🗄️  Output: ClickHouse")
                
                # Process based on command
                success = processor.process_single_era(command, output_file, separate_files, export_type)
                
                if success:
                    processed_count += 1
                    self.progress_data["processed_eras"].append(era_number)
                    print(f"✅ Successfully processed era {era_number}")
                else:
                    failed_count += 1
                    failed_eras.append(era_number)
                    self.progress_data["failed_eras"].append(era_number)
                    print(f"❌ Failed to process era {era_number}")
                
                # Cleanup downloaded file
                self.cleanup_era(local_path)
                
                # Save progress periodically
                self._save_progress()
                
            except Exception as e:
                print(f"❌ Error processing era {era_number}: {e}")
                failed_count += 1
                failed_eras.append(era_number)
                self.progress_data["failed_eras"].append(era_number)
                
                # Try to cleanup on error
                if 'local_path' in locals() and local_path:
                    self.cleanup_era(local_path)
        
        # Final summary
        print(f"\n{'='*60}")
        print(f"🎉 REMOTE PROCESSING COMPLETE!")
        print(f"{'='*60}")
        print(f"✅ Successfully processed: {processed_count}/{len(available_eras)} eras")
        print(f"❌ Failed: {failed_count} eras")
        
        if failed_eras:
            print(f"❌ Failed eras: {failed_eras}")
        
        # Save final progress
        self._save_progress()
        
        summary = {
            "success": True,
            "total_eras": len(available_eras),
            "processed_count": processed_count,
            "failed_count": failed_count,
            "failed_eras": failed_eras,
            "progress_file": str(self.progress_file)
        }
        
        return summary

    def _generate_era_output_filename(self, base_output: str, era_number: int) -> str:
        """Generate output filename for era"""
        # Extract directory and base name
        output_dir = os.path.dirname(base_output) if os.path.dirname(base_output) else ""
        base_name = os.path.splitext(os.path.basename(base_output))[0]
        extension = os.path.splitext(base_output)[1] or ".json"
        
        # Generate filename with era number
        filename = f"{base_name}_era_{era_number:05d}{extension}"
        
        if output_dir:
            return os.path.join(output_dir, filename)
        else:
            return filename
    
    def list_progress(self) -> Dict[str, Any]:
        """Get current progress information"""
        return {
            "network": self.progress_data.get("network"),
            "processed_eras": len(self.progress_data.get("processed_eras", [])),
            "failed_eras": len(self.progress_data.get("failed_eras", [])),
            "last_run": self.progress_data.get("last_run"),
            "progress_file": str(self.progress_file)
        }
    
    def clear_progress(self):
        """Clear all progress data"""
        self.progress_data = {
            "network": self.network,
            "processed_eras": [],
            "failed_eras": [],
            "last_run": None
        }
        if self.progress_file.exists():
            self.progress_file.unlink()
        print("🗑️  Progress data cleared")


def load_env_file(env_file_path: str = '.env'):
    """Load environment variables from .env file"""
    if os.path.exists(env_file_path):
        print(f"📁 Loading environment from {env_file_path}")
        with open(env_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Only set if not already in environment
                    if key not in os.environ:
                        os.environ[key] = value
                        print(f"   ✅ Set {key}")
    else:
        print(f"   ℹ️  No .env file found at {env_file_path}")


def get_remote_era_downloader() -> RemoteEraDownloader:
    """
    Factory function to create RemoteEraDownloader from environment variables
    
    Environment variables:
        ERA_BASE_URL: Base URL for era files (required)
        ERA_DOWNLOAD_DIR: Directory for temporary downloads (optional)
        ERA_CLEANUP_AFTER_PROCESS: Whether to delete files after processing (default: true)
        ERA_MAX_RETRIES: Maximum retry attempts (default: 3)
    
    Returns:
        Configured RemoteEraDownloader instance
    """
    # Try to load .env file first
    load_env_file()
    
    base_url = os.getenv('ERA_BASE_URL')
    if not base_url:
        raise ValueError("ERA_BASE_URL environment variable is required")
    
    download_dir = os.getenv('ERA_DOWNLOAD_DIR')
    cleanup = os.getenv('ERA_CLEANUP_AFTER_PROCESS', 'true').lower() == 'true'
    max_retries = int(os.getenv('ERA_MAX_RETRIES', '3'))
    
    # Network will be set when initializing
    return RemoteEraDownloader(
        base_url=base_url,
        network='',  # Will be set by caller
        download_dir=download_dir,
        cleanup=cleanup,
        max_retries=max_retries
    )