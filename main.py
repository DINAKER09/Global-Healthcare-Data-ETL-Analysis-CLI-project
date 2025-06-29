import argparse
import logging
import configparser
from api_client import APIClient
from data_transformer import DataTransformer
from mysql_handler import MySQLHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def main():
    config = load_config()

    # Initialize components
    db = MySQLHandler(**config['mysql'])
    #db.create_tables()
    api = APIClient(config['api']['base_url'], config['api']['api_key'])
    transformer = DataTransformer()

    # Setup CLI interface
    parser = argparse.ArgumentParser(description="Healthcare Hospital Data CLI")
    subparsers = parser.add_subparsers(dest='command')

    # Fetch data subcommand
    fetch = subparsers.add_parser('fetch_data', help='Fetch hospital data from API')
    fetch.add_argument('--state', required=True, help='U.S. state code (e.g., FL, CA)')

    # Query commands
    query = subparsers.add_parser('query_data')
    query.add_argument('query_type')
    query.add_argument('state', nargs='?')
    query.add_argument('metric', nargs='?')
    query.add_argument('n', nargs='?', type=int)

    subparsers.add_parser('list_tables')
    subparsers.add_parser('drop_tables')

    args = parser.parse_args()

    if args.command == 'fetch_data':
        # Fetch from API-Ninjas /v1/hospitals using state filter
        params = {"state": args.state}
        raw = api.fetch_data("hospitals", params)
        if not raw:
            logging.warning("No data fetched from API.")
            return
        df = transformer.clean_and_transform(raw)
        db.insert_data('hospitals', df)

    elif args.command == 'query_data':
        if args.query_type == 'total_hospitals':
            results = db.query("SELECT COUNT(*) FROM hospitals WHERE state = %s", (args.state,))
            print(f"Total hospitals in {args.state}: {results[0][0]}")
        elif args.query_type == 'top_n_by_beds':
            results = db.query(
                "SELECT name, bedcount FROM hospitals WHERE bedcount IS NOT NULL ORDER BY bedcount DESC LIMIT %s", (args.n,)
            )
            for row in results:
                print(row)

    elif args.command == 'list_tables':
        print("Available tables:", db.list_tables())

    elif args.command == 'drop_tables':
        db.drop_tables()
        print("Dropped table: hospitals")

    db.close()

if __name__ == '__main__':
    main()
