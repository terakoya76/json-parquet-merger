# json-parquet-merger

A TypeScript CLI tool that merges multiple JSON files of the same format into a single Parquet file.

## Features

- 🚀 Fast processing and batch processing support
- 📊 Automatic schema inference
- ✅ Schema validation option
- 🔍 File pattern filtering
- 📝 Detailed progress display
- 🛠️ Customizable batch size

## Usage

### Basic usage
```bash
# Merge all JSON files in the directory
json-parquet-merger -i ./data -o output.parquet

# Convert a single JSON file
json-parquet-merger -i data.json -o output.parquet
```

### Options

```bash
bashjson-parquet-merger [options]

Options:
  -i, --input <path>      Input directory or file path (required)
  -o, --output <path>     Output Parquet file path (required)
  -p, --pattern <regex>   Regular expression for filtering JSON files
  --validate              Verify that all records have the same schema
  -b, --batch-size <num>  Batch size for processing records (default: 1000)
  -h, --help              Display help
```

### Usage examples
```bash
# Pattern filtering
json-parquet-merger -i ./data -o users.parquet -p “user_.*\\.json”

# Run with schema validation enabled
json-parquet-merger -i ./data -o output.parquet --validate

# Custom batch size
json-parquet-merger -i ./data -o output.parquet -b 5000

# Display help and samples
json-parquet-merger help
```

### Input JSON file requirements

- All JSON files must have the same schema (same key structure).
- JSON files can be written in array format or single object format.
- Nested objects are stored as JSON strings.

### Sample JSON File

user1.json:
```json
[
  {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30,
    "active": true,
    "created_at": "2024-01-15T10:00:00Z"
  },
  {
    "id": 2,
    "name": "Bob",
    "email": "bob@example.com",
    "age": 25,
    "active": false,
    "created_at": "2024-01-16T10:00:00Z"
  }
]
```

user2.json:
```json
[
  {
    "id": 3,
    "name": "Charlie",
    "email": "charlie@example.com",
    "age": 35,
    "active": true,
    "created_at": "2024-01-17T10:00:00Z"
  }
]
```
