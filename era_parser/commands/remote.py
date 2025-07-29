from typing import List, Tuple, Optional

from .base import BaseCommand
from ..ingestion.remote_downloader import get_remote_era_downloader

class RemoteCommand(BaseCommand):
    """Handler for remote era processing operations using unified state management"""
    
    def execute(self, args: List[str]) -> None:
        """Execute remote processing command"""
        print(f"🔍 RemoteCommand.execute() received args: {args}")
        
        if not args:
            print("❌ Remote command requires arguments")
            return
        
        # Check if this is a utility command that starts with --
        first_arg = args[0]
        print(f"🔍 First arg: '{first_arg}', starts with --: {first_arg.startswith('--')}")
        
        if first_arg.startswith('--'):
            print(f"🔍 Taking utility command path for: {first_arg}")
            if args[0] == "--remote-progress":
                self._handle_remote_progress(args[1:])
            elif args[0] == "--remote-clear":
                self._handle_remote_clear(args[1:])
            elif args[0] == "--clean-failed":
                self._handle_clean_failed(args[1:])
            elif args[0] == "--force-clean":
                self._handle_force_clean(args[1:])
            elif args[0] == "--optimize":
                self._handle_optimize_tables(args[1:])
            else:
                print(f"❌ Unknown remote utility command: {args[0]}")
            return
        
        # Standard remote processing: network era_range [command] [options]
        print(f"🔍 Taking standard remote processing path")
        self._handle_remote_processing(args)
    
    def _handle_remote_progress(self, args: List[str]) -> None:
        """Handle remote progress display"""
        if not self.validate_required_args(args, 1, "era-parser --remote-progress <network>"):
            return
        
        network = args[0]
        
        try:
            downloader = get_remote_era_downloader(network)
            downloader.network = network
            downloader.progress_file = downloader.download_dir / f".era_progress_{network}.json"
            downloader.progress_data = downloader._load_progress()
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
            downloader = get_remote_era_downloader(network)
            downloader.network = network
            downloader.progress_file = downloader.download_dir / f".era_progress_{network}.json"
            downloader.progress_data = downloader._load_progress()
            print(f"✅ Cleared progress for {network}")
        except ValueError as e:
            print(f"❌ Configuration error: {e}")

    def _handle_remote_processing(self, args: List[str]) -> None:
        """Handle main remote processing"""
        if len(args) < 2:
            print("Usage: era-parser --remote <network> <era_range> <command> [<o>] [--separate] [--force] [--export clickhouse]")
            print("   or: era-parser --remote <network> <era_range> --download-only")
            return
        
        network = args[0]
        era_range = args[1]
        
        # Check for download-only mode anywhere in args
        if "--download-only" in args:
            self._handle_download_only(network, era_range)
            return
        
        if len(args) < 3:
            print("Usage: era-parser --remote <network> <era_range> <command> [<o>] [--separate] [--force] [--export clickhouse]")
            return
        
        command = args[2]
        base_output = args[3] if len(args) > 3 and not args[3].startswith('--') else "output"
        
        # Parse flags
        flags, _ = self.parse_flags(args[3:])
        separate_files = flags['separate']
        force = '--force' in args
        export_type = self.get_export_type(flags)
        
        try:
            downloader = get_remote_era_downloader(network)
            downloader.network = network
            
            result = downloader.process_era_range(
                *self._parse_era_range(era_range),
                command=command,
                base_output=base_output,
                separate_files=separate_files,
                force=force,
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
        print(f"📥 Download-only mode for {network} era range {era_range}")
        
        try:
            downloader = get_remote_era_downloader(network)
            
            start_era, end_era = self._parse_era_range(era_range)
            available_eras = downloader.discover_era_files(start_era, end_era)
            
            downloaded_count = 0
            for era_number, url in available_eras:
                local_path = downloader.download_era(era_number, url)
                if local_path:
                    downloaded_count += 1
                    print(f"✅ Downloaded era {era_number} to {local_path}")
                else:
                    print(f"❌ Failed to download era {era_number}")
            
            print(f"🎉 Downloaded {downloaded_count}/{len(available_eras)} era files")
            
        except Exception as e:
            self.handle_error(e, "download-only processing")
    
    def _parse_era_range(self, era_range: str) -> Tuple[int, Optional[int]]:
        """Parse era range string into start and end values"""
        try:
            # Handle leading zeros and various formats
            era_range = era_range.strip()
            
            if '+' in era_range:
                start_str = era_range.replace('+', '').strip()
                start_era = int(start_str)
                print(f"📊 Parsed era range '{era_range}' as: start={start_era}, end=None (open-ended)")
                return start_era, None
            elif '-' in era_range:
                start_str, end_str = era_range.split('-', 1)
                start_era, end_era = int(start_str.strip()), int(end_str.strip())
                print(f"📊 Parsed era range '{era_range}' as: start={start_era}, end={end_era}")
                return start_era, end_era
            else:
                era = int(era_range.strip())
                print(f"📊 Parsed era range '{era_range}' as: start={era}, end={era} (single era)")
                return era, era
        except (ValueError, IndexError) as e:
            print(f"❌ Invalid era range format: '{era_range}'")
            print(f"Expected formats: '1000', '1000-2000', or '1000+'")
            raise ValueError(f"Invalid era range format: '{era_range}'") from e

    def _handle_clean_failed(self, args: List[str]) -> None:
        """Handle cleaning failed eras using unified state manager"""
        if not self.validate_required_args(args, 1, "era-parser --remote --clean-failed <network>"):
            return
        
        network = args[0]
        
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            
            failed_eras = state_manager.clean_failed_eras(network)
            
            if failed_eras:
                print(f"🧹 Cleaned {len(failed_eras)} failed eras: {failed_eras}")
            else:
                print(f"✅ No failed eras found for {network}")
                
        except Exception as e:
            print(f"❌ Failed to clean failed eras: {e}")

    def _handle_force_clean(self, args: List[str]) -> None:
        """Handle force cleaning specific eras using unified state manager"""
        if not self.validate_required_args(args, 2, "era-parser --remote --force-clean <network> <era_range>"):
            return
        
        network = args[0]
        era_range = args[1]
        
        try:
            start_era, end_era = self._parse_era_range(era_range)
            if end_era is None:
                end_era = start_era
            
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            
            cleaned_count = 0
            for era_number in range(start_era, end_era + 1):
                try:
                    state_manager.clean_era_completely(network, era_number)
                    cleaned_count += 1
                except Exception as e:
                    print(f"⚠️  Could not clean era {era_number}: {e}")
            
            print(f"🧹 Force cleaned {cleaned_count} eras from {start_era} to {end_era}")
            
        except Exception as e:
            print(f"❌ Failed to force clean eras: {e}")

    def _handle_optimize_tables(self, args: List[str]) -> None:
        """Handle table optimization using unified state manager"""
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            state_manager.optimize_tables()
            print("✅ Table optimization completed")
        except Exception as e:
            print(f"❌ Table optimization failed: {e}")