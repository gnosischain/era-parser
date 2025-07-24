import os
import json
import requests
import tempfile
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urljoin
import time

class RemoteEraDownloader:
    """Downloads and processes era files from remote URLs"""
    
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
        
        print(f"🌐 Remote Era Downloader initialized")
        print(f"   Base URL: {self.base_url}")
        print(f"   Network: {self.network}")
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
    
    def _construct_era_url(self, era_number: int) -> str:
        """Construct URL for era file"""
        # Format: network-XXXXX-hash.era (we don't know the hash, so we'll search)
        era_str = f"{era_number:05d}"
        return f"{self.base_url}/{self.network}-{era_str}-"
    
    def _find_era_file(self, era_number: int) -> Optional[str]:
        """Find the actual era file URL by testing common hash patterns"""
        era_str = f"{era_number:05d}"
        
        # First, try to get a directory listing or use a pattern
        # For S3, we'll try common approaches
        
        # Method 1: Try a HEAD request to see if we can find the file
        # We'll try a few common hash lengths (8 chars is most common)
        base_pattern = f"{self.base_url}/{self.network}-{era_str}-"
        
        # Try to find the file by making a request without the hash and see if we get redirected
        # or by trying common hash patterns
        test_url = f"{base_pattern}*.era"  # This won't work directly
        
        # More practical approach: Try a few requests with different strategies
        session = requests.Session()
        session.headers.update({'User-Agent': 'era-parser/1.0'})
        
        # Strategy 1: Try common hash patterns (if we know them)
        # Strategy 2: Try to list the bucket (if public)
        # Strategy 3: Make educated guesses
        
        # For now, let's implement a brute force approach for the last few characters
        # This is not ideal but works for the immediate need
        
        # Try requesting the base URL pattern and see what we get
        try:
            # Try without hash first (might work for some setups)
            test_url = f"{self.base_url}/{self.network}-{era_str}.era"
            response = session.head(test_url, timeout=10)
            if response.status_code == 200:
                return test_url
        except:
            pass
        
        # If that doesn't work, we need a different strategy
        # For S3 buckets, we might need to implement XML parsing of bucket listings
        # For now, let's return None and handle this in the caller
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
    
    def discover_era_files(self, start_era: int, end_era: Optional[int] = None) -> List[Tuple[int, str]]:
        """
        Discover available era files in range
        
        Args:
            start_era: Starting era number
            end_era: Ending era number (None = discover until not found)
            
        Returns:
            List of (era_number, url) tuples
        """
        print(f"🔍 Discovering era files starting from era {start_era}")
        
        available_eras = []
        current_era = start_era
        consecutive_failures = 0
        max_consecutive_failures = 5  # Stop after 5 consecutive failures
        
        session = requests.Session()
        session.headers.update({'User-Agent': 'era-parser/1.0'})
        
        while True:
            if end_era is not None and current_era > end_era:
                break
                
            # Try to find this era file
            era_str = f"{current_era:05d}"
            
            # Method 1: Try to get S3 bucket listing (if public)
            found_url = self._discover_era_file_with_hash(session, current_era)
            
            if found_url:
                available_eras.append((current_era, found_url))
                consecutive_failures = 0
                print(f"   ✅ Found era {current_era}: {found_url}")
            else:
                consecutive_failures += 1
                print(f"   ❌ Era {current_era} not found (consecutive failures: {consecutive_failures})")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"   🛑 Stopping discovery after {max_consecutive_failures} consecutive failures")
                    break
            
            current_era += 1
            
            # Small delay to be nice to the server
            time.sleep(0.1)
        
        print(f"🎯 Discovery complete: found {len(available_eras)} era files")
        return available_eras
    
    def _discover_era_file_with_hash(self, session: requests.Session, era_number: int) -> Optional[str]:
        """
        Try to discover the actual era file URL with hash suffix
        
        For S3 buckets, files are named like: network-XXXXX-hash.era
        We need to find the hash part.
        """
        era_str = f"{era_number:05d}"
        
        # Method 1: Try without hash first (some setups might work)
        simple_url = f"{self.base_url}/{self.network}-{era_str}.era"
        try:
            response = session.head(simple_url, timeout=10)
            if response.status_code == 200:
                return simple_url
        except:
            pass
        
        # Method 2: Try to get S3 bucket listing
        try:
            # For S3, we can try to list objects with a prefix
            # This works if the bucket allows public listing
            list_url = f"{self.base_url}/?list-type=2&prefix={self.network}-{era_str}-"
            response = session.get(list_url, timeout=10)
            
            if response.status_code == 200:
                content = response.text
                
                # Try to parse as S3 XML response
                if '<ListBucketResult' in content or '<Contents>' in content:
                    import xml.etree.ElementTree as ET
                    try:
                        root = ET.fromstring(content)
                        
                        # Handle different XML namespaces
                        namespaces = {
                            '': 'http://s3.amazonaws.com/doc/2006-03-01/',
                            's3': 'http://s3.amazonaws.com/doc/2006-03-01/'
                        }
                        
                        # Try to find Contents elements
                        for ns_prefix in ['', 's3:']:
                            contents = root.findall(f'.//{ns_prefix}Contents', namespaces if ns_prefix else {})
                            for content_elem in contents:
                                key_elem = content_elem.find(f'{ns_prefix}Key', namespaces if ns_prefix else {})
                                if key_elem is not None:
                                    key = key_elem.text
                                    if key and key.startswith(f"{self.network}-{era_str}-") and key.endswith('.era'):
                                        return f"{self.base_url}/{key}"
                        
                        # Also try without namespace
                        for content_elem in root.findall('.//Contents'):
                            key_elem = content_elem.find('Key')
                            if key_elem is not None:
                                key = key_elem.text
                                if key and key.startswith(f"{self.network}-{era_str}-") and key.endswith('.era'):
                                    return f"{self.base_url}/{key}"
                                    
                    except ET.ParseError:
                        # If XML parsing fails, try regex on the raw content
                        import re
                        pattern = rf'<Key>({self.network}-{era_str}-[a-f0-9]{{8}}.era)</Key>'
                        matches = re.findall(pattern, content, re.IGNORECASE)
                        if matches:
                            return f"{self.base_url}/{matches[0]}"
                
                # If not XML, try to find the filename in plain text or HTML
                import re
                # Look for filenames that match our pattern
                pattern = rf'{self.network}-{era_str}-[a-f0-9]{{8}}\.era'
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    return f"{self.base_url}/{matches[0]}"
        
        except Exception as e:
            print(f"   🔍 Bucket listing failed: {e}")
        
        # Method 3: Try alternative S3 listing format
        try:
            # Try the older S3 API format
            list_url = f"{self.base_url}/?prefix={self.network}-{era_str}-&max-keys=10"
            response = session.get(list_url, timeout=10)
            
            if response.status_code == 200:
                content = response.text
                import re
                pattern = rf'{self.network}-{era_str}-[a-f0-9]{{8}}\.era'
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    return f"{self.base_url}/{matches[0]}"
        except:
            pass
        
        # Method 4: Try some educated guesses based on common patterns
        # This is a last resort - try a few common hash formats
        print(f"   🎲 Trying educated guesses for era {era_number}")
        
        # Since we don't know the hash, we can't really guess it
        # But let's try a few approaches that might work for some servers
        
        return None
    
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
            
            # Get era processing progress
            summary = state_manager.get_processing_summary(network)
            
            processed_eras = set()
            
            # Query for fully completed eras in range
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
            
            # Build query with range filters
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
        
        # Discover available eras
        available_eras = self.discover_era_files(start_era, end_era)
        
        if not available_eras:
            print("❌ No era files found in the specified range")
            return {"success": False, "processed_count": 0, "failed_count": 0}
        
        # Filter out already processed eras if resuming
        if resume:
            processed_eras_file = set(self.progress_data.get("processed_eras", []))
            available_eras = [(era, url) for era, url in available_eras if era not in processed_eras_file]
            print(f"📋 Resume mode: {len(available_eras)} eras remaining after filtering file processed ones")
        
        # Filter out ClickHouse processed eras using new state manager
        if export_type == "clickhouse":
            try:
                state_processed_eras = self.get_processed_eras_from_state(self.network, start_era, end_era)
                available_eras = [(era, url) for era, url in available_eras if era not in state_processed_eras]
                print(f"📋 Era state filter: {len(available_eras)} eras remaining after filtering state processed ones")
            except Exception as e:
                print(f"⚠️  Could not filter by era state: {e}")
        elif processed_eras:  # Legacy ClickHouse filter
            available_eras = [(era, url) for era, url in available_eras if era not in processed_eras]
            print(f"📋 Legacy ClickHouse filter: {len(available_eras)} eras remaining")
        
        # Process each era
        processed_count = 0
        failed_count = 0
        failed_eras = []
        
        # Import here to avoid circular dependencies
        from ..cli import EraParserCLI
        
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
                
                # Process using existing CLI logic
                cli = EraParserCLI()
                cli.setup(local_path)
                
                # Generate output filename
                if export_type == "file":
                    output_file = self._generate_era_output_filename(base_output, era_number)
                    print(f"   📂 Output: {output_file}")
                else:
                    output_file = "clickhouse_output"  # Not used for ClickHouse
                    print(f"   🗄️  Output: ClickHouse")
                
                # Process based on command
                success = cli._process_single_era(command, output_file, separate_files, export_type)
                
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