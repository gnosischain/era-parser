"""Remote era processing commands"""

from typing import List, Tuple, Optional

from .base import BaseCommand
from ..ingestion.remote_downloader import get_remote_era_downloader

class RemoteCommand(BaseCommand):
    """Handler for remote era processing operations"""
    
    def execute(self, args: List[str]) -> None:
        """Execute remote processing command"""
        if not args:
            print("❌ Remote command requires arguments")
            return
        
        # Check if this is a utility command that starts with --
        if args[0].startswith('--'):
            if args[0] == "--remote-progress":
                self._handle_remote_progress(args[1:])
            elif args[0] == "--remote-clear":
                self._handle_remote_clear(args[1:])
            else:
                print(f"❌ Unknown remote utility command: {args[0]}")
            return
        
        # Standard remote processing: network era_range [command] [options]
        self._handle_remote_processing(args)
    
    def _handle_remote_progress(self, args: List[str]) -> None:
        """Handle remote progress display"""
        if not self.validate_required_args(args, 1, "era-parser --remote-progress <network>"):
            return
        
        network = args[0]
        
        try:
            downloader = get_remote_era_downloader()
            downloader.network = network
            progress = downloader.list_progress()
            
            print(f"📊 Remote Processing Progress ({network})")
            print(f"   Processed eras: {progress['processed_eras']}")
            print(f"   Failed eras: {progress['failed_eras']}")
            if progress['last_run']:
                import datetime
                last_run = datetime.datetime.fromtimestamp(progress['last_run'])
                print(f"   Last run: {last_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Progress file: {progress['progress_file']}")
            
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
    
    def _handle_remote_clear(self, args: List[str]) -> None:
        """Handle remote progress clearing"""
        if not self.validate_required_args(args, 1, "era-parser --remote-clear <network>"):
            return
        
        network = args[0]
        
        try:
            downloader = get_remote_era_downloader()
            downloader.network = network
            downloader.clear_progress()
            print(f"✅ Cleared progress for {network}")
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
    
    def _handle_remote_processing(self, args: List[str]) -> None:
        """Handle main remote processing"""
        if len(args) < 2:
            print("Usage: era-parser --remote <network> <era_range> <command> [<o>] [--separate] [--resume] [--export clickhouse]")
            print("   or: era-parser --remote <network> <era_range> --download-only")
            return
        
        network = args[0]
        era_range = args[1]
        
        # Check for download-only mode anywhere in args
        if "--download-only" in args:
            self._handle_download_only(network, era_range)
            return
        
        if len(args) < 3:
            print("Usage: era-parser --remote <network> <era_range> <command> [<o>] [--separate] [--resume] [--export clickhouse]")
            return
        
        command = args[2]
        base_output = args[3] if len(args) > 3 and not args[3].startswith('--') else "output"
        
        # Parse flags
        flags, _ = self.parse_flags(args[3:])
        separate_files = flags['separate']
        resume = flags['resume']
        export_type = self.get_export_type(flags)
        
        try:
            result = self._process_remote_eras(
                network=network,
                era_range=era_range,
                command=command,
                base_output=base_output,
                separate_files=separate_files,
                resume=resume,
                export_type=export_type
            )
            
            if result["success"]:
                print(f"🎉 Remote processing completed successfully!")
                print(f"   Processed: {result['processed_count']} eras")
                if result['failed_count'] > 0:
                    print(f"   Failed: {result['failed_count']} eras")
            else:
                print(f"❌ Remote processing failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            self.handle_error(e, "remote processing")
    
    def _handle_download_only(self, network: str, era_range: str) -> None:
        """Handle download-only mode"""
        result = self._process_remote_eras(
            network=network,
            era_range=era_range,
            command="",
            base_output="",
            download_only=True
        )
        if result["success"]:
            print(f"🎉 Downloaded {result['downloaded_count']}/{result['total_available']} era files")
    
    def _parse_era_range(self, era_range: str) -> Tuple[int, Optional[int]]:
        """Parse era range string into start and end values"""
        if '+' in era_range:
            start_era = int(era_range.replace('+', ''))
            return start_era, None
        elif '-' in era_range:
            start_str, end_str = era_range.split('-', 1)
            return int(start_str), int(end_str)
        else:
            era = int(era_range)
            return era, era
    
    def _process_remote_eras(self, network: str, era_range: str, command: str, 
                           base_output: str, separate_files: bool = False,
                           download_only: bool = False, resume: bool = False,
                           export_type: str = "file") -> dict:
        """Process remote era files"""
        print(f"🌐 Remote Era Processing")
        print(f"   Network: {network}")
        print(f"   Era range: {era_range}")
        print(f"   Command: {command}")
        print(f"   Export type: {export_type}")
        
        start_era, end_era = self._parse_era_range(era_range)
        
        try:
            downloader = get_remote_era_downloader()
            downloader.network = network
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
            print("💡 Make sure to set ERA_BASE_URL environment variable")
            return {"success": False, "error": str(e)}
        
        if export_type == "clickhouse":
            try:
                from ..export.clickhouse_service import ClickHouseService
                ch_service = ClickHouseService()
                processed_eras = set(ch_service.get_processed_eras(network, start_era, end_era))
                print(f"📋 Found {len(processed_eras)} already processed eras in ClickHouse")
            except Exception as e:
                print(f"⚠️  Could not check ClickHouse status: {e}")
                processed_eras = set()
        else:
            processed_eras = set()
        
        if download_only:
            print("📥 Download-only mode")
            available_eras = downloader.discover_era_files(start_era, end_era)
            
            downloaded_count = 0
            for era_number, url in available_eras:
                local_path = downloader.download_era(era_number, url)
                if local_path:
                    downloaded_count += 1
                    print(f"✅ Downloaded era {era_number} to {local_path}")
                else:
                    print(f"❌ Failed to download era {era_number}")
            
            return {
                "success": True,
                "mode": "download_only",
                "downloaded_count": downloaded_count,
                "total_available": len(available_eras)
            }
        else:
            return downloader.process_era_range(
                start_era=start_era,
                end_era=end_era,
                command=command,
                base_output=base_output,
                separate_files=separate_files,
                resume=resume,
                export_type=export_type,
                processed_eras=processed_eras
            )