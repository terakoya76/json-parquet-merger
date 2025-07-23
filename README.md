# json-parquet-merger

A TypeScript CLI tool that merges multiple JSON files of the same format into a single Parquet file.

## Features

- 🚀 Fast processing and batch processing support
- 📊 Automatic schema inference from all input files
- ✅ Schema validation option
- 🔍 File pattern filtering with regex support
- 📝 Detailed progress display with real-time feedback
- 🛠️ Customizable batch size
- 🗜️ Multiple compression options (uncompressed, gzip, snappy, brotli)
- 📁 Support for both single files and directory processing
- 🔄 Robust error handling and file validation

## Installation

```bash
# Install globally
npm install -g json-parquet-merger

# Or install locally with pnpm (recommended for development)
pnpm install json-parquet-merger

# Or using npm
npm install json-parquet-merger
```

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
json-parquet-merger [options]

Options:
  -i, --input <path>           Input directory or file path (required)
  -o, --output <path>          Output Parquet file path (required)
  -p, --pattern <regex>        Regular expression for filtering JSON files
  --validate                   Verify that all records have the same schema
  -b, --batch-size <number>    Batch size for processing records (default: 1000)
  -c, --compression <type>     Compression type: uncompressed, gzip, snappy, brotli (default: uncompressed)
  -V, --version                Display version number
  -h, --help                   Display help information
```

### Usage examples
```bash
# Pattern filtering
json-parquet-merger -i ./data -o users.parquet -p "user_.*\\.json"

# Run with schema validation enabled
json-parquet-merger -i ./data -o output.parquet --validate

# Custom batch size
json-parquet-merger -i ./data -o output.parquet -b 5000

# With compression
json-parquet-merger -i ./data -o output.parquet -c gzip

# Combined options
json-parquet-merger -i ./data -o output.parquet -p "user_.*\\.json" --validate -b 2000 -c snappy

# Display help
json-parquet-merger --help

# Show version
json-parquet-merger --version
```

### Input JSON file requirements

- JSON files can contain data with any schema structure
- Schema is automatically inferred from all input files (not just the first one)
- JSON files can be written in array format or single object format
- Missing fields in some files are automatically handled (marked as optional)
- Nested objects and arrays are automatically converted to JSON strings
- All field types are auto-detected: string, number (int64/double), boolean, timestamps
- Complex objects/arrays are serialized to JSON strings for storage

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

## Schema Inference

### Supported Data Types

| JSON Type | Parquet Type | Notes |
|-----------|--------------|-------|
| `string` | `UTF8` | Text data |
| `integer` | `INT64` | Whole numbers |
| `float` | `DOUBLE` | Decimal numbers |
| `boolean` | `BOOLEAN` | true/false values |
| `Date` | `TIMESTAMP_MILLIS` | Date objects |
| `object/array` | `UTF8` | Serialized as JSON strings |

## Compression Options

The tool supports multiple compression algorithms:

- **uncompressed** (default): No compression, fastest processing
- **gzip**: Good compression ratio, moderate speed
- **snappy**: Fast compression with reasonable ratio
- **brotli**: Best compression ratio, slower processing
