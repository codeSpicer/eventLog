## Requirements

- **Go version:** 1.20 or higher (recommended: latest stable release)

## Building the Executable

From the project root, run:

```sh
# Initialize Go module
go mod init eventlog

# Add the SQLite dependency
go get github.com/mattn/go-sqlite3

# Verify dependencies
go mod tidy
```

```sh
go build -o eventlog main.go
```

This will produce an executable named `eventlog` in your project directory.

## Generating Test Records

To generate test event records (e.g., 1 million events):

```sh
mkdir -p data

# Generate small test data (1,000 events) - for quick testing
go run data/generate_test_data.go data/events_small.txt 1000

# Generate medium test data (100,000 events) - for performance testing
go run data/generate_test_data.go data/events_medium.txt 100000

# Generate full test data (1,000,000 events) - for final testing
go run data/generate_test_data.go data/events_1M.txt 1000000
```

## Recording Events Using the Binary

To record the generated events into your database (e.g., `events.db`):

```sh
./eventlog record data/events_small.txt

```

This command will read events from `data/events_data.txt` and store them in the database.

## Querying Events

To query all events:

```sh
# users 0-1999 are heavy users 2000-10000 are light users
./eventlog query 0 --type=login

./eventlog query 0 --from=2023-08-14T10:00:00Z --to=2023-08-14T11:00:00Z
```

This will print all stored events of a user.

## Performance testing

```sh
# clean previous db
rm -f events.db*

time ./eventlog record data/events_1M.txt

time ./eventlog query 0 > /dev/null

time ./eventlog query 0 --from=2023-08-14T10:00:00Z --to=2023-08-14T12:00:00Z > /dev/null

```
