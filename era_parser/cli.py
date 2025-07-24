import sys
import os

def print_help():
    """Print comprehensive help information"""
    print("Era Parser - Ethereum Beacon Chain Era File Parser")
    print("")
    print("LOCAL FILE COMMANDS:")
    print("  era-parser <era_file> block <slot>                    # Parse single block")
    print("  era-parser <era_file> all-blocks <output_file>        # All blocks")
    print("  era-parser <era_file> all-blocks <output_file> --separate  # Separate files")
    print("  era-parser <era_file> transactions <output_file>      # Transaction data only")
    print("  era-parser <era_file> withdrawals <output_file>       # Withdrawal data only")
    print("  era-parser <era_file> attestations <output_file>      # Attestation data only")
    print("  era-parser <era_file> sync_aggregates <output_file>   # Sync aggregate data only")
    print("  era-parser <era_file> stats                           # Show era statistics")
    print("")
    print("CLICKHOUSE EXPORT:")
    print("  era-parser <era_file> all-blocks --export clickhouse  # Export to ClickHouse")
    print("  era-parser <era_file> transactions --export clickhouse # Export transactions to ClickHouse")
    print("  era-parser <era_file> sync_aggregates --export clickhouse # Export sync aggregates to ClickHouse")
    print("")
    print("REMOTE ERA PROCESSING:")
    print("  era-parser --remote <network> <era_range> <command> <o>           # Process remote eras")
    print("  era-parser --remote <network> <era_range> <command> <o> --separate  # Separate files")
    print("  era-parser --remote <network> <era_range> <command> --export clickhouse # Remote to ClickHouse")
    print("  era-parser --remote <network> <era_range> --download-only              # Download only")
    print("  era-parser --remote <network> <era_range> <command> <o> --resume # Resume processing")
    print("")
    print("ERA STATE MANAGEMENT:")
    print("  era-parser --era-status <network|all>                 # Show era processing status")
    print("  era-parser --era-failed <network|all> [limit]         # Show failed datasets")
    print("  era-parser --era-cleanup [timeout_minutes]            # Clean stale processing")
    print("  era-parser --era-check <era_file>                     # Check specific era status")
    print("")
    print("REMOTE UTILITY COMMANDS:")
    print("  era-parser --remote-progress <network>                # Show remote progress")
    print("  era-parser --remote-clear <network>                   # Clear remote progress")
    print("")
    print("ERA RANGE FORMATS:")
    print("  1082        # Single era")
    print("  1082-1100   # Era range (inclusive)")
    print("  1082+       # From era 1082 until no more files found")
    print("")
    print("NOTES:")
    print("  - Era state management provides granular dataset tracking")
    print("  - ClickHouse export ALWAYS creates separate tables (like --separate)")
    print("  - Parquet with --separate creates one file per data type")
    print("  - All nested data is fully extracted and preserved")
    print("  - SIMPLIFIED: Only single timestamp_utc per table for monthly partitioning")


def main():
    """Main CLI entry point with command routing"""
    if len(sys.argv) < 2:
        print_help()
        sys.exit(1)
    
    try:
        # Route to appropriate command handler
        first_arg = sys.argv[1]
        
        if first_arg == "--batch":
            from .commands.batch import BatchCommand
            command = BatchCommand()
            command.execute(sys.argv[2:])
            
        elif first_arg == "--remote":
            from .commands.remote import RemoteCommand
            command = RemoteCommand()
            command.execute(sys.argv[2:])  # Pass args without --remote
            
        elif first_arg in ["--era-status", "--era-failed", "--era-cleanup", "--era-check"]:
            from .commands.state import StateCommand
            command = StateCommand()
            command.execute(sys.argv[1:])  # Include the command flag
            
        elif first_arg in ["--remote-progress", "--remote-clear"]:
            from .commands.remote import RemoteCommand
            command = RemoteCommand()
            command.execute(sys.argv[1:])  # Include the command flag
            
        elif first_arg.startswith('--'):
            print(f"❌ Unknown command: {first_arg}")
            print_help()
            sys.exit(1)
            
        else:
            # Local file processing
            from .commands.local import LocalCommand
            command = LocalCommand()
            command.execute(sys.argv[1:])
    
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()