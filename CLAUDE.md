# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a TypeScript CLI tool that merges multiple JSON files with identical schemas into a single Parquet file. The tool uses `parquetjs` for Parquet file operations and `commander` for CLI interface.

## Core Architecture

- **Main entry point**: `src/index.ts` - Contains the `JsonParquetMerger` class and CLI setup
- **Single-file architecture**: All core logic is contained in the main file
- **Key components**:
  - `JsonParquetMerger` class: Handles file discovery, schema inference, validation, and batch processing
  - Schema inference: Automatically detects field types from first JSON file
  - Batch processing: Processes records in configurable batches (default 1000)
  - File pattern filtering: Supports regex filtering of input files

## Development Commands

```bash
# Build the project
pnpm run build

# Run CLI locally during development
pnpm run cli

# Run tests
pnpm test

# Run tests in watch mode
pnpm test:watch

# Lint and fix code formatting
pnpm run lint
```

## Testing

- Uses Vitest as the test framework
- Test files: `src/index.test.ts` and `src/test/integration.test.ts`
- Test data located in `src/test/data/`
- Run single test file: `npx vitest run src/index.test.ts`

## Package Manager

This project uses `pnpm` (required version >=10.0.0). Always use `pnpm` commands instead of `npm` or `yarn`.

## CLI Usage

```bash
# Basic usage
json-parquet-merger -i ./data -o output.parquet

# With options
json-parquet-merger -i ./data -o output.parquet -p "user_.*\.json" --validate -b 5000
```

## Schema Handling

- Schema is automatically inferred from the first JSON file
- Supports basic types: string, number (int64/double), boolean, timestamp
- Complex objects/arrays are serialized to JSON strings
- All fields are marked as optional in the Parquet schema
- Schema validation can be enabled with `--validate` flag

## When Making Changes

- Always run linting after changes: `pnpm run lint`
- Build and test before committing: `pnpm run build && pnpm test`
- The project uses Google TypeScript Style (gts) for formatting