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
    print("")
    print("REMOTE ERA PROCESSING:")
    print("  era-parser --remote <network> <era_range> <command> <o>           # Process remote eras")
    print("  era-parser --remote <network> <era_range> <command> <o> --separate  # Separate files")
    print("  era-parser --remote <network> <era_range> <command> --export clickhouse # Remote to ClickHouse")
    print("  era-parser --remote <network> <era_range> <command> <o> --force  # Force reprocess")
    print("  era-parser --remote <network> <era_range> --download-only              # Download only")
    print("")
    print("ERA STATE MANAGEMENT:")
    print("  era-parser --era-status <network> [era_range]         # Show era completion status")
    print("  era-parser --clean-failed-eras <network>              # Clean failed eras")
    print("  era-parser --remote --clean-failed <network>          # Clean failed eras (remote)")
    print("  era-parser --remote --force-clean <network> <range>   # Force clean era range")
    print("  era-parser --remote --optimize                        # Optimize ClickHouse tables")
    print("")
    print("MIGRATION COMMANDS:")
    print("  era-parser --migrate status                           # Show migration status")
    print("  era-parser --migrate run [version]                    # Run migrations")
    print("  era-parser --migrate list                             # List available migrations")
    print("")
    print("ERA RANGE FORMATS:")
    print("  1082        # Single era")
    print("  1082-1100   # Era range (inclusive)")
    print("  1082+       # From era 1082 until no more files found")
    print("")
    print("SIMPLIFIED USAGE:")
    print("  era-parser --remote gnosis 1000-2000 all-blocks --export clickhouse  # Skip completed")
    print("  era-parser --remote gnosis 1500-1600 all-blocks --export clickhouse --force  # Reprocess all")
    print("  era-parser --clean-failed-eras gnosis")
    print("")
    print("NOTES:")
    print("  - Normal mode automatically skips completed eras")
    print("  - --force cleans and reprocesses everything") 
    print("  - Failed eras can be cleaned and retried easily")


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
            print(f"🔍 CLI routing to remote command with args: {sys.argv[2:]}")
            from .commands.remote import RemoteCommand
            command = RemoteCommand()
            command.execute(sys.argv[2:])  # Pass args without --remote
            
        elif first_arg in ["--era-status", "--era-failed", "--era-cleanup", "--era-check", "--clean-failed-eras"]:
            from .commands.state import StateCommand
            command = StateCommand()
            command.execute(sys.argv[1:])  # Include the command flag
            
        elif first_arg in ["--remote-progress", "--remote-clear"]:
            from .commands.remote import RemoteCommand
            command = RemoteCommand()
            command.execute(sys.argv[1:])  # Include the command flag
            
        elif first_arg == "--migrate":
            from .commands.migrate import MigrateCommand
            command = MigrateCommand()
            command.execute(sys.argv[2:])  # Pass args without --migrate
            
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