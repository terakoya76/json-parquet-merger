import {Command} from 'commander';
import {glob} from 'glob';
import chalk from 'chalk';
import * as fs from 'fs/promises';
import * as path from 'path';
import {ParquetWriter, ParquetSchema} from '@dsnp/parquetjs';

export interface ProcessingOptions {
  input: string;
  output: string;
  pattern?: string;
  validate: boolean;
  batchSize: number;
}

export interface JsonRecord {
  [key: string]: unknown;
}

export class JsonParquetMerger {
  private options: ProcessingOptions;
  private inferredSchema: ParquetSchema | null = null;
  private processedCount = 0;

  constructor(options: ProcessingOptions) {
    this.options = options;
  }

  async run(): Promise<void> {
    try {
      console.log(chalk.blue('🚀 Starting JSON to Parquet merger...'));

      // Discover files
      const files = await this.discoverFiles();
      console.log(
        chalk.green(`📁 Found ${files.length} JSON files to process`),
      );

      if (files.length === 0) {
        console.log(chalk.yellow('⚠️  No JSON files found matching criteria'));
        return;
      }

      // Infer schema from all files
      await this.inferSchema(files);
      console.log(chalk.green('🔍 Schema inferred successfully'));

      // Process files in batches
      await this.processFiles(files);

      console.log(
        chalk.green(
          `✅ Successfully merged ${this.processedCount} records into ${this.options.output}`,
        ),
      );
    } catch (error) {
      console.error(
        chalk.red('❌ Error:'),
        error instanceof Error ? error.message : String(error),
      );
      throw new Error('Processing failed');
    }
  }

  private async discoverFiles(): Promise<string[]> {
    let searchPattern: string;

    // Check if input is file or directory
    const inputStat = await fs.stat(this.options.input).catch(() => null);

    if (inputStat?.isFile()) {
      return [this.options.input];
    }

    if (inputStat?.isDirectory()) {
      searchPattern = path.join(this.options.input, '**/*.json');
    } else {
      throw new Error(`Input path ${this.options.input} does not exist`);
    }

    let files = await glob(searchPattern);

    // Apply pattern filtering if specified
    if (this.options.pattern) {
      const regex = new RegExp(this.options.pattern);
      files = files.filter(file => regex.test(path.basename(file)));
    }

    return files;
  }

  private async inferSchema(files: string[]): Promise<void> {
    try {
      // Collect all unique field names and their types across all files
      const schemaFields: Record<string, any> = {};
      const allFields = new Set<string>();

      // First pass: collect all field names from all files
      for (const file of files) {
        const content = await fs.readFile(file, 'utf-8');
        let jsonData: JsonRecord[];

        try {
          const parsed = JSON.parse(content);
          jsonData = Array.isArray(parsed) ? parsed : [parsed];
        } catch (error) {
          throw new Error(`Failed to parse JSON from ${file}: ${error}`);
        }

        if (jsonData.length === 0) {
          continue; // Skip empty files
        }

        // Collect all field names from this file
        for (const record of jsonData) {
          Object.keys(record).forEach(key => allFields.add(key));
        }
      }

      if (allFields.size === 0) {
        throw new Error('No fields found in any of the input files');
      }

      // Second pass: for each field, find the first non-null value across all files
      for (const fieldName of allFields) {
        let inferredType = null;

        // Search across all files until we find a non-null value for this field
        fileLoop: for (const file of files) {
          const content = await fs.readFile(file, 'utf-8');
          let jsonData: JsonRecord[];

          try {
            const parsed = JSON.parse(content);
            jsonData = Array.isArray(parsed) ? parsed : [parsed];
          } catch (error) {
            continue; // Skip files that can't be parsed
          }

          for (const record of jsonData) {
            const value = record[fieldName];
            if (value !== null && value !== undefined) {
              if (typeof value === 'string') {
                inferredType = {type: 'UTF8', optional: true};
                break fileLoop;
              } else if (typeof value === 'number') {
                inferredType = Number.isInteger(value)
                  ? {type: 'INT64', optional: true}
                  : {type: 'DOUBLE', optional: true};
                break fileLoop;
              } else if (typeof value === 'boolean') {
                inferredType = {type: 'BOOLEAN', optional: true};
                break fileLoop;
              } else if (value instanceof Date) {
                inferredType = {type: 'TIMESTAMP_MILLIS', optional: true};
                break fileLoop;
              } else {
                // Convert objects/arrays to JSON strings
                inferredType = {type: 'UTF8', optional: true};
                break fileLoop;
              }
            }
          }
        }

        // If all values are null across all files, default to UTF8
        schemaFields[fieldName] = inferredType || {
          type: 'UTF8',
          optional: true,
        };
      }

      this.inferredSchema = new ParquetSchema(schemaFields);
    } catch (error) {
      console.error(
        chalk.red(
          `❌ Error in inferSchema() for files: ${Array.isArray(files) ? files.join(', ') : files}`,
        ),
      );
      throw error;
    }
  }

  private async validateSchema(records: JsonRecord[]): Promise<boolean> {
    if (!this.options.validate) return true;

    const schemaKeys = Object.keys(this.inferredSchema!.fields);
    let isValid = true;
    const validationErrors: string[] = [];

    for (const record of records) {
      const recordKeys = Object.keys(record);

      // Check for missing fields
      const missingFields = schemaKeys.filter(key => !(key in record));
      if (missingFields.length > 0) {
        isValid = false;
        const errorMsg = `Missing fields in record: ${missingFields.join(', ')}`;
        validationErrors.push(errorMsg);
        console.warn(chalk.yellow(`⚠️  ${errorMsg}`));
      }

      // Check for extra fields
      const extraFields = recordKeys.filter(key => !schemaKeys.includes(key));
      if (extraFields.length > 0) {
        isValid = false;
        const errorMsg = `Extra fields in record: ${extraFields.join(', ')}`;
        validationErrors.push(errorMsg);
        console.warn(chalk.yellow(`⚠️  ${errorMsg}`));
      }
    }

    return isValid;
  }

  private transformRecord(record: JsonRecord): JsonRecord {
    const transformed: JsonRecord = {};

    for (const [key, value] of Object.entries(record)) {
      if (value === null || value === undefined) {
        transformed[key] = null;
      } else if (typeof value === 'object' && !(value instanceof Date)) {
        // Convert objects/arrays to JSON strings
        transformed[key] = JSON.stringify(value);
      } else {
        transformed[key] = value;
      }
    }

    return transformed;
  }

  private async processFiles(files: string[]): Promise<void> {
    if (!this.inferredSchema) {
      throw new Error('Schema not inferred');
    }
    const writer = await ParquetWriter.openFile(
      this.inferredSchema,
      this.options.output,
    );
    let currentBatch: JsonRecord[] = [];
    let currentFile = '';

    try {
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        currentFile = file;

        console.log(
          chalk.blue(`📄 Processing file ${i + 1}/${files.length}: ${file}`),
        );

        const content = await fs.readFile(file, 'utf-8');
        let jsonData: JsonRecord[];

        try {
          const parsed = JSON.parse(content);
          jsonData = Array.isArray(parsed) ? parsed : [parsed];
        } catch (error) {
          console.error(chalk.red(`❌ Failed to parse ${file}:`), error);
          continue;
        }

        // Validate schema if enabled
        const isValid = await this.validateSchema(jsonData);
        if (!isValid) {
          console.error(
            chalk.red(`❌ Schema validation failed for ${file}, skipping file`),
          );
          continue;
        }

        // Process records in batches
        for (const record of jsonData) {
          const transformedRecord = this.transformRecord(record);
          currentBatch.push(transformedRecord);

          if (currentBatch.length >= this.options.batchSize) {
            await this.writeBatch(writer, currentBatch);
            currentBatch = [];
          }
        }

        // Show progress
        const progress = (((i + 1) / files.length) * 100).toFixed(1);
        console.log(
          chalk.gray(
            `   Progress: ${progress}% (${this.processedCount} records processed)`,
          ),
        );
      }

      // Write remaining records
      if (currentBatch.length > 0) {
        await this.writeBatch(writer, currentBatch);
      }
    } catch (error) {
      console.error(chalk.red(`❌ Error processing file: ${currentFile}`));
      throw error;
    } finally {
      await writer.close();
    }
  }

  private async writeBatch(
    writer: ParquetWriter,
    batch: JsonRecord[],
  ): Promise<void> {
    for (const record of batch) {
      await writer.appendRow(record);
    }
    this.processedCount += batch.length;
    console.log(chalk.gray(`   Wrote batch of ${batch.length} records`));
  }
}

async function main(): Promise<void> {
  const program = new Command();

  program
    .name('json-parquet-merger')
    .description(
      'Merge multiple JSON files with identical schemas into a single Parquet file',
    )
    .version('1.0.0')
    .requiredOption('-i, --input <path>', 'Input directory or file path')
    .requiredOption('-o, --output <path>', 'Output Parquet file path')
    .option(
      '-p, --pattern <regex>',
      'Regular expression pattern for filtering JSON files',
    )
    .option('--validate', 'Enable schema validation across all files', false)
    .option(
      '-b, --batch-size <number>',
      'Batch size for processing records',
      '1000',
    );

  program.parse();
  const options = program.opts() as ProcessingOptions;

  // Convert batch size to number
  options.batchSize = parseInt(options.batchSize.toString(), 10);

  if (isNaN(options.batchSize) || options.batchSize < 1) {
    console.error(chalk.red('❌ Batch size must be a positive number'));
    throw new Error('Invalid batch size');
  }

  // Validate output directory exists
  const outputDir = path.dirname(options.output);
  try {
    await fs.access(outputDir);
  } catch {
    console.error(
      chalk.red(`❌ Output directory does not exist: ${outputDir}`),
    );
    throw new Error('Output directory does not exist');
  }

  // Create and run merger
  const merger = new JsonParquetMerger(options);
  await merger.run();
}

if (require.main === module) {
  main().catch(console.error);
}
