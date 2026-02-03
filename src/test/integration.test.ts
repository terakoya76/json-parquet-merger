import * as fs from 'fs/promises';
import {ParquetWriter, ParquetSchema} from '@dsnp/parquetjs';
import * as path from 'path';
import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  MockInstance,
} from 'vitest';

import {JsonParquetMerger, CompressionType} from '../index';

/**
 * Type for a mocked ParquetWriter where methods are replaced with mock instances.
 * This provides proper typing while allowing the object to be used where ParquetWriter is expected.
 */
type MockedParquetWriter = {
  [K in keyof ParquetWriter]: ParquetWriter[K] extends (
    ...args: infer A
  ) => infer R
    ? MockInstance<ParquetWriter[K]> & ((...args: A) => R)
    : ParquetWriter[K];
};

/**
 * Creates a mock ParquetWriter for testing.
 * This factory centralizes the mock creation and type assertion.
 */
function createMockParquetWriter(): MockedParquetWriter {
  return {
    appendRow: vi.fn(),
    close: vi.fn(),
    schema: {} as ParquetSchema,
    envelopeWriter: null,
    rowBuffer: {} as ParquetWriter['rowBuffer'],
    rowGroupSize: 1000,
    closed: false,
    userMetadata: {},
    setMetadata: vi.fn(),
    setRowGroupSize: vi.fn(),
    setPageSize: vi.fn(),
  } as MockedParquetWriter;
}

// Mock only the ParquetWriter since we don't want to create actual Parquet files
vi.mock('@dsnp/parquetjs', () => ({
  ParquetWriter: {
    openFile: vi.fn(),
  },
  ParquetSchema: vi.fn().mockImplementation(function (this: any, fields: any) {
    this.fields = fields;
  }),
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

describe('JsonParquetMerger Integration Tests', () => {
  let mockWriter: MockedParquetWriter;
  const testDataDir = path.resolve(__dirname, './data');
  const outputPath = path.join(testDataDir, './output/test.parquet');

  beforeEach(async () => {
    // Create output directory
    await fs.mkdir(path.dirname(outputPath), {recursive: true});

    mockWriter = createMockParquetWriter();

    vi.mocked(ParquetWriter.openFile).mockResolvedValue(mockWriter);
    vi.clearAllMocks();

    // Suppress console output during tests unless specifically testing it
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(async () => {
    // Clean up output files
    try {
      await fs.unlink(outputPath);
    } catch {
      // File might not exist, ignore
    }
    vi.restoreAllMocks();
  });

  describe('End-to-end processing with real files', () => {
    it('should process multiple JSON files and merge them', async () => {
      const merger = new JsonParquetMerger({
        input: testDataDir,
        output: outputPath,
        pattern: 'data\\d+',
        validate: false,
        batchSize: 2,
        compression: 'UNCOMPRESSED' as CompressionType,
      });
      await merger.run();

      // Verify schema inference
      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        timestamp: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
        event: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
        userId: {type: 'INT64', compression: 'UNCOMPRESSED', optional: true},
      });

      // Verify data processing
      expect(mockWriter.appendRow).toHaveBeenCalledTimes(3); // 2 from data1.json + 1 from data2.json
      expect(mockWriter.close).toHaveBeenCalledOnce();

      // Verify records content
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        timestamp: '2023-01-01T00:00:00Z',
        event: 'login',
        userId: 1001,
      });
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        timestamp: '2023-01-01T01:30:00Z',
        event: 'purchase',
        userId: 1002,
      });
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        timestamp: '2023-01-01T02:15:00Z',
        event: 'logout',
        userId: 1001,
      });
    });

    it('should process single JSON file with nested objects', async () => {
      const merger = new JsonParquetMerger({
        input: path.join(testDataDir, 'users.json'),
        output: outputPath,
        validate: false,
        batchSize: 10,
        compression: 'UNCOMPRESSED' as CompressionType,
      });
      await merger.run();

      // Verify schema inference includes nested object as UTF8
      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', compression: 'UNCOMPRESSED', optional: true},
        name: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
        age: {type: 'INT64', compression: 'UNCOMPRESSED', optional: true},
        active: {type: 'BOOLEAN', compression: 'UNCOMPRESSED', optional: true},
        metadata: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
      });

      // Verify data processing
      expect(mockWriter.appendRow).toHaveBeenCalledTimes(2);
      expect(mockWriter.close).toHaveBeenCalledOnce();

      // Verify records content
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        id: 1,
        name: 'John Doe',
        age: 30,
        active: true,
        metadata: JSON.stringify({
          department: 'Engineering',
          skills: ['JavaScript', 'TypeScript', 'Node.js'],
        }),
      });
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        id: 2,
        name: 'Jane Smith',
        age: 28,
        active: false,
        metadata: JSON.stringify({
          department: 'Design',
          skills: ['UI/UX', 'Figma', 'Sketch'],
        }),
      });
    });

    it('should process single object JSON file', async () => {
      const merger = new JsonParquetMerger({
        input: path.join(testDataDir, 'single-object.json'),
        output: outputPath,
        validate: false,
        batchSize: 1,
        compression: 'UNCOMPRESSED' as CompressionType,
      });
      await merger.run();

      // Verify schema inference
      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', compression: 'UNCOMPRESSED', optional: true},
        title: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
        value: {type: 'DOUBLE', compression: 'UNCOMPRESSED', optional: true},
        active: {type: 'BOOLEAN', compression: 'UNCOMPRESSED', optional: true},
      });

      // Verify data processing
      expect(mockWriter.appendRow).toHaveBeenCalledTimes(1);
      expect(mockWriter.close).toHaveBeenCalledOnce();

      // Verify records content
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        id: 999,
        title: 'Single Record',
        value: 42.5,
        active: false,
      });
    });

    it('should validate schema and skip invalid files when validation is enabled', async () => {
      const merger = new JsonParquetMerger({
        input: path.join(testDataDir, 'invalid-schema.json'),
        output: outputPath,
        validate: true,
        batchSize: 10,
        compression: 'UNCOMPRESSED' as CompressionType,
      });

      // The merger will try to create a Parquet file with zero rows, which will fail
      await expect(merger.run()).rejects.toThrow();

      // No records should be written due to validation failure
      expect(mockWriter.appendRow).not.toHaveBeenCalled();
    });

    it('should handle mixed data types correctly', async () => {
      const merger = new JsonParquetMerger({
        input: path.join(testDataDir, 'products.json'),
        output: outputPath,
        validate: false,
        batchSize: 5,
        compression: 'UNCOMPRESSED' as CompressionType,
      });
      await merger.run();

      // Verify schema inference for mixed types
      expect(vi.mocked(ParquetSchema)).toHaveBeenCalledWith({
        id: {type: 'INT64', compression: 'UNCOMPRESSED', optional: true},
        name: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
        price: {type: 'DOUBLE', compression: 'UNCOMPRESSED', optional: true},
        available: {
          type: 'BOOLEAN',
          compression: 'UNCOMPRESSED',
          optional: true,
        },
        specs: {type: 'UTF8', compression: 'UNCOMPRESSED', optional: true},
      });

      // Verify data processing
      expect(mockWriter.appendRow).toHaveBeenCalledTimes(2);
      expect(mockWriter.close).toHaveBeenCalledOnce();

      // Verify records content
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        id: 101,
        name: 'Laptop Pro',
        price: 1299.99,
        available: true,
        specs: JSON.stringify({
          cpu: 'Intel i7',
          ram: '16GB',
          storage: '512GB SSD',
        }),
      });
      expect(mockWriter.appendRow).toHaveBeenCalledWith({
        id: 102,
        name: 'Wireless Mouse',
        price: 29.99,
        available: true,
        specs: JSON.stringify({
          type: 'wireless',
          battery: 'AAA',
          dpi: 1600,
        }),
      });
    });
  });

  describe('Error handling with real files', () => {
    it('should handle non-existent input files gracefully', async () => {
      const merger = new JsonParquetMerger({
        input: path.join(testDataDir, 'nonexistent.json'),
        output: outputPath,
        validate: false,
        batchSize: 10,
        compression: 'UNCOMPRESSED' as CompressionType,
      });

      await expect(merger.run()).rejects.toThrow();
    });
  });
});
