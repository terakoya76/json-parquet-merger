import {vol} from 'memfs';
import {ParquetSchema} from '@dsnp/parquetjs';
import {describe, it, expect, vi, beforeEach, afterEach} from 'vitest';

import {JsonParquetMerger, CompressionType} from './index';

// Mock external dependencies before importing the module under test
vi.mock('fs/promises', () => import('memfs').then(({fs}) => fs.promises));
vi.mock('glob');
vi.mock('@dsnp/parquetjs', () => ({
  ParquetWriter: {
    openFile: vi.fn(),
  },
  ParquetSchema: vi.fn().mockImplementation(fields => ({fields})),
}));
vi.mock('chalk', () => ({
  default: {
    blue: vi.fn(text => text),
    green: vi.fn(text => text),
    yellow: vi.fn(text => text),
    red: vi.fn(text => text),
    gray: vi.fn(text => text),
  },
}));

describe('JsonParquetMerger', () => {
  beforeEach(async () => {
    // Reset memfs before each test
    vol.reset();

    vi.clearAllMocks();
  });

  afterEach(() => {
    vol.reset();
    vi.clearAllMocks();
  });

  describe('constructor', () => {
    it('should initialize with provided options', () => {
      const mockOptions = {
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      };
      const merger = new JsonParquetMerger(mockOptions);
      expect(merger).toBeDefined();

      expect(merger['options']).toEqual(mockOptions);
      expect(merger['inferredSchema']).toBeNull();
      expect(merger['processedCount']).toBe(0);
    });
  });

  describe('discoverFiles', () => {
    it('should return single file when input is a file', async () => {
      vol.fromJSON({
        '/test/input.json': '{}',
      });

      const merger = new JsonParquetMerger({
        input: '/test/input.json',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      const result = await merger['discoverFiles']();
      expect(result).toEqual(['/test/input.json']);
    });

    it('should discover JSON files in directory', async () => {
      // Create directory structure in memfs
      vol.fromJSON({
        '/test/input/file1.json': '{}',
        '/test/input/file2.json': '{}',
        '/test/input/subdirectory/file3.json': '{}',
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });

      const {glob} = await import('glob');
      vi.mocked(glob).mockResolvedValue([
        '/test/input/file1.json',
        '/test/input/file2.json',
        '/test/input/subdirectory/file3.json',
      ]);

      const result = await merger['discoverFiles']();
      expect(result).toEqual([
        '/test/input/file1.json',
        '/test/input/file2.json',
        '/test/input/subdirectory/file3.json',
      ]);
      expect(glob).toHaveBeenCalledWith('/test/input/**/*.json');
    });

    it('should filter files by pattern when provided', async () => {
      const patternOptions = {
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        pattern: 'data\\d+',
        compression: 'UNCOMPRESSED' as const,
      };
      const patternMerger = new JsonParquetMerger(patternOptions);

      // Create directory structure in memfs
      vol.fromJSON({
        '/test/input/data1.json': '{}',
        '/test/input/data2.json': '{}',
        '/test/input/other.json': '{}',
      });

      const {glob} = await import('glob');
      vi.mocked(glob).mockResolvedValue([
        '/test/input/data1.json',
        '/test/input/data2.json',
        '/test/input/other.json',
      ]);

      const result = await patternMerger['discoverFiles']();

      expect(result).toEqual([
        '/test/input/data1.json',
        '/test/input/data2.json',
      ]);
    });

    it('should throw error when input path does not exist', async () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });

      // Don't create the path in memfs, so it doesn't exist
      await expect(merger['discoverFiles']()).rejects.toThrow(
        'Input path /test/input does not exist',
      );
    });
  });

  describe('inferSchema', () => {
    it('should infer schema from JSON array', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([
          {id: 1, name: 'John', age: 30, active: true},
          {id: 2, name: 'Jane', age: 25, active: false},
        ]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await merger['inferSchema'](['/test/file.json']);
      expect(merger['inferredSchema']).toBeDefined();

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        name: {type: 'UTF8', optional: true, compression: 'UNCOMPRESSED'},
        age: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        active: {type: 'BOOLEAN', optional: true, compression: 'UNCOMPRESSED'},
      });
    });

    it('should infer schema from single object', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify({id: 1, name: 'Test', value: 42}),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await merger['inferSchema'](['/test/file.json']);

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        name: {type: 'UTF8', optional: true, compression: 'UNCOMPRESSED'},
        value: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
      });
    });

    it('should handle nested objects as UTF8', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([
          {
            id: 1,
            metadata: {tags: ['a', 'b'], config: {enabled: true}},
          },
        ]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await merger['inferSchema'](['/test/file.json']);

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        metadata: {type: 'UTF8', optional: true, compression: 'UNCOMPRESSED'},
      });
    });

    it('should handle different data types correctly', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([
          {
            stringField: 'test',
            intField: 42,
            floatField: 3.14,
            boolField: true,
            nullField: null,
            dateField: new Date().toISOString(), // JSON doesn't preserve Date objects
            objectField: {nested: 'value'},
          },
        ]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await merger['inferSchema'](['/test/file.json']);

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        stringField: {
          type: 'UTF8',
          optional: true,
          compression: 'UNCOMPRESSED',
        },
        intField: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        floatField: {
          type: 'DOUBLE',
          optional: true,
          compression: 'UNCOMPRESSED',
        },
        boolField: {
          type: 'BOOLEAN',
          optional: true,
          compression: 'UNCOMPRESSED',
        },
        nullField: {type: 'UTF8', optional: true, compression: 'UNCOMPRESSED'},
        dateField: {type: 'UTF8', optional: true, compression: 'UNCOMPRESSED'}, // ISO string, not Date object
        objectField: {
          type: 'UTF8',
          optional: true,
          compression: 'UNCOMPRESSED',
        },
      });
    });

    it('should correctly infer INT64 type when first value is null and subsequent values are numbers', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([
          {id: 1, score: null, name: 'John'},
          {id: 2, score: 85, name: 'Jane'},
          {id: 3, score: 92, name: 'Bob'},
        ]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await merger['inferSchema'](['/test/file.json']);

      // This test verifies the fix: score should be INT64 even when first record has null
      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        score: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        name: {type: 'UTF8', optional: true, compression: 'UNCOMPRESSED'},
      });
    });

    it('should correctly infer types across multiple files when first file has all null values', async () => {
      vol.fromJSON({
        '/test/file1.json': JSON.stringify([
          {id: 1, score: null, active: null, name: 'John'},
          {id: 2, score: null, active: null, name: 'Jane'},
        ]),
        '/test/file2.json': JSON.stringify([
          {id: 3, score: 85.5, active: true, name: 'Bob'},
          {id: 4, score: 92, active: false, name: 'Alice'},
        ]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await merger['inferSchema'](['/test/file1.json', '/test/file2.json']);

      // This test verifies the cross-file fix: types should be inferred from file2
      // even when file1 has all null values
      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', optional: true, compression: 'UNCOMPRESSED'},
        score: {type: 'DOUBLE', optional: true, compression: 'UNCOMPRESSED'}, // 85.5 is a double
        active: {type: 'BOOLEAN', optional: true, compression: 'UNCOMPRESSED'},
        name: {type: 'UTF8', optional: true, compression: 'UNCOMPRESSED'},
      });
    });

    it('should throw error for invalid JSON', async () => {
      vol.fromJSON({
        '/test/file.json': 'invalid json',
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await expect(merger['inferSchema'](['/test/file.json'])).rejects.toThrow(
        'Failed to parse JSON from /test/file.json',
      );
    });

    it('should throw error for empty data', async () => {
      vol.fromJSON({
        '/test/file.json': '[]',
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await expect(merger['inferSchema'](['/test/file.json'])).rejects.toThrow(
        'No fields found in any of the input files',
      );
    });
  });

  describe('validateSchema', () => {
    it('should return true and not validate when validate option is false', async () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      merger['inferredSchema'] = new ParquetSchema({
        id: {type: 'INT64', optional: true},
        name: {type: 'UTF8', optional: true},
      });

      const result = await merger['validateSchema']([{id: 1, extra: 'field'}]);
      expect(result).toBe(true);
    });

    it('should return false and warn about missing fields when validate is true', async () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: true,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      merger['inferredSchema'] = new ParquetSchema({
        id: {type: 'INT64', optional: true},
        name: {type: 'UTF8', optional: true},
      });

      const result = await merger['validateSchema']([{id: 1}]);
      expect(result).toBe(false);
    });

    it('should return false and warn about extra fields when validate is true', async () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: true,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      merger['inferredSchema'] = new ParquetSchema({
        id: {type: 'INT64', optional: true},
      });

      const result = await merger['validateSchema']([
        {id: 1, name: 'John', extra: 'field'},
      ]);
      expect(result).toBe(false);
    });

    it('should return true when all fields match the schema', async () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: true,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      merger['inferredSchema'] = new ParquetSchema({
        id: {type: 'INT64', optional: true},
        name: {type: 'UTF8', optional: true},
      });

      vi.spyOn(console, 'warn').mockImplementation(() => {});
      const result = await merger['validateSchema']([{id: 1, name: 'John'}]);
      expect(result).toBe(true);
    });

    it('should return false when multiple validation issues exist', async () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: true,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      merger['inferredSchema'] = new ParquetSchema({
        id: {type: 'INT64', optional: true},
        name: {type: 'UTF8', optional: true},
        age: {type: 'INT64', optional: true},
      });

      const result = await merger['validateSchema']([
        {id: 1, extra: 'field'}, // missing name, age; has extra field
        {name: 'John'}, // missing id, age
      ]);
      expect(result).toBe(false);
    });
  });

  describe('transformRecord', () => {
    it('should transform nested objects to JSON strings', () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });

      const result = merger['transformRecord']({
        id: 1,
        metadata: {tags: ['a', 'b'], count: 5},
        array: [1, 2, 3],
      });
      expect(result).toEqual({
        id: 1,
        metadata: '{"tags":["a","b"],"count":5}',
        array: '[1,2,3]',
      });
    });

    it('should handle null and undefined values', () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });

      const result = merger['transformRecord']({
        nullField: null,
        undefinedField: undefined,
        value: 42,
      });
      expect(result).toEqual({
        nullField: null,
        undefinedField: null,
        value: 42,
      });
    });

    it('should preserve Date objects', () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });

      const date = new Date('2023-01-01');
      const result = merger['transformRecord']({date, id: 1});
      expect(result).toEqual({date, id: 1});
    });

    it('should preserve primitive values', () => {
      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });

      const record = {
        string: 'test',
        number: 42,
        boolean: true,
        float: 3.14,
      };
      const result = merger['transformRecord'](record);
      expect(result).toEqual(record);
    });
  });

  describe('writeBatch', () => {
    it('should write records to ParquetWriter and update count', async () => {
      const mockWriter = {
        appendRow: vi.fn(),
        schema: {} as any,
        envelopeWriter: null,
        rowBuffer: {} as any,
        rowGroupSize: 1000,
        closed: false,
        userMetadata: {},
        close: vi.fn(),
        setMetadata: vi.fn(),
        setRowGroupSize: vi.fn(),
        setPageSize: vi.fn(),
      } as any;
      const batch = [{id: 1}, {id: 2}];
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as const,
      });
      await merger['writeBatch'](mockWriter, batch);

      expect(mockWriter.appendRow).toHaveBeenCalledTimes(2);
      expect(mockWriter.appendRow).toHaveBeenCalledWith({id: 1});
      expect(mockWriter.appendRow).toHaveBeenCalledWith({id: 2});
      expect(merger['processedCount']).toBe(2);
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Wrote batch of 2 records'),
      );

      consoleSpy.mockRestore();
    });
  });

  describe('compression options', () => {
    it('should use UNCOMPRESSED compression by default', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([{id: 1, name: 'test'}]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'UNCOMPRESSED' as CompressionType,
      });
      await merger['inferSchema'](['/test/file.json']);

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', compression: 'UNCOMPRESSED', optional: true},
        name: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
      });
    });

    it('should use GZIP compression when specified', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([{id: 1, name: 'test'}]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'GZIP' as CompressionType,
      });
      await merger['inferSchema'](['/test/file.json']);

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', compression: 'GZIP', optional: true},
        name: {type: 'UTF8', compression: 'GZIP', optional: true},
      });
    });

    it('should use SNAPPY compression when specified', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([{id: 1, name: 'test'}]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'SNAPPY' as CompressionType,
      });
      await merger['inferSchema'](['/test/file.json']);

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', compression: 'SNAPPY', optional: true},
        name: {type: 'UTF8', compression: 'SNAPPY', optional: true},
      });
    });

    it('should use BROTLI compression when specified', async () => {
      vol.fromJSON({
        '/test/file.json': JSON.stringify([{id: 1, name: 'test'}]),
      });

      const merger = new JsonParquetMerger({
        input: '/test/input',
        output: '/test/output.parquet',
        validate: false,
        batchSize: 1000,
        compression: 'BROTLI' as CompressionType,
      });
      await merger['inferSchema'](['/test/file.json']);

      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', compression: 'BROTLI', optional: true},
        name: {type: 'UTF8', compression: 'BROTLI', optional: true},
      });
    });
  });
});
