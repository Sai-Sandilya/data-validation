import argparse
import sys
import os

# Add the backend directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from services.sync_service import perform_sync

def main():
    parser = argparse.ArgumentParser(description="Sync data between source and target databases")
    parser.add_argument("--table", type=str, required=True, help="Table name to sync")
    args = parser.parse_args()
    
    try:
        result = perform_sync(args.table)
        print(f"✅ Sync completed: {result}")
    except Exception as e:
        print(f"❌ Sync failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
