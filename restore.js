/**
 * PostgreSQL Database Restoration Script
 * 
 * This script performs the following operations:
 * 1. Downloads compressed backup files from AWS S3
 * 2. Decompresses and processes the CSV files
 * 3. Handles data type conversions for PostgreSQL compatibility
 * 4. Restores data to the database using PostgreSQL's COPY command
 * 
 * IMPORTANT: This script restores data from a specific batch file.
 * Make sure to specify the correct yearMonth and batchNo before running.
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { from } from 'pg-copy-streams';
import readline from 'readline';
import moment from 'moment';
import fs from 'fs';
import zlib from 'zlib';
import { parse } from 'csv-parse';

// Import utility functions and configurations
import { s3 } from './util.js';
import { pool } from './util.js';
import { S3_BUCKET_NAME } from './util.js';
import { BACKUP_PATH } from './util.js';

// ===================== CONFIGURATION SECTION =====================
// IMPORTANT: Update these values before running the script

const tableName = '<table_name>';        // Replace with your actual table name

// --------------SET THE YearMonth and BatchNo------------
const yearMonth = '2022-03';              // Format: YYYY-MM
const batchNo = 1;                        // Batch number to restore
// ---------------------------------------------------------

const batchName = `backup_${yearMonth}_batch${batchNo}.csv.gz`;

// ===================== DATABASE SCHEMA CONFIGURATION =====================
// Uncomment and modify the following if you need to connect to a specific schema
// (other than the default 'public' schema)
// pool.on('connect', async (client) => {
//   await client.query(`SET search_path TO your_schema_name`);
// });

// Construct S3 key path for the backup file
const s3Key = `db_backup/${tableName}/${yearMonth}/${batchName}`;

console.log('\n\n\nRestoring started.........................');

/**
 * Main function to download and restore a backup file from S3
 * Handles the complete restoration workflow with proper cleanup
 * 
 * @param {string} S3_BUCKET_NAME - Name of the S3 bucket
 * @param {string} key - S3 key (path) of the backup file
 * @param {string} tableName - Name of the table to restore data to
 */
async function downloadAndRestoreS3File(S3_BUCKET_NAME, key, tableName) {
    const localFilePath = `${BACKUP_PATH}/temp-file.csv`;
    const processedFilePath = `${BACKUP_PATH}/processed-temp-file.csv`;

    try {
        // Step 1: Download compressed backup file from S3
        console.log(`Downloading file from S3: ${key}`);
        const params = { Bucket: S3_BUCKET_NAME, Key: key };
        const command = new GetObjectCommand(params);
        const data = await s3.send(command);

        // Step 2: Decompress the gzipped file
        console.log('Decompressing file...');
        const decompressed = zlib.gunzipSync(await streamToBuffer(data.Body));
        fs.writeFileSync(localFilePath, decompressed);

        // Step 3: Process CSV file for database compatibility
        console.log('Processing file for time zone compatibility...');
        await preprocessCsvFile(localFilePath, processedFilePath);

        // Step 4: Restore data to the database
        console.log('File processed. Restoring to database...');
        await restoreCsvToTable(tableName, processedFilePath);

        console.log('Restoration completed successfully.');
    } catch (err) {
        console.error('Error during download and restore process:', err);
    } finally {
        // Clean up temporary files
        if (fs.existsSync(localFilePath)) {
            fs.unlinkSync(localFilePath);
        }
        if (fs.existsSync(processedFilePath)) {
            fs.unlinkSync(processedFilePath);
        }
    }
}

/**
 * Preprocess CSV file for database compatibility
 * Handles data type conversions and formatting issues
 * 
 * @param {string} inputFilePath - Path to the input CSV file
 * @param {string} outputFilePath - Path to the processed output CSV file
 */
async function preprocessCsvFile(inputFilePath, outputFilePath) {
    console.log('Processing file:', inputFilePath);

    const inputStream = fs.createReadStream(inputFilePath);
    const outputStream = fs.createWriteStream(outputFilePath);

    let headers = null;

    // Parse CSV with proper configuration for data exported from PostgreSQL
    const parser = inputStream.pipe(
        parse({
            columns: true,              // Use first row as headers
            skip_empty_lines: true,     // Skip empty lines
            relax_column_count: true,   // Allow variable column count
            quote: '"'                  // Treat double quotes as escape characters
        })
    );

    // Process each record in the CSV file
    for await (const record of parser) {
        if (!headers) {
            // Write headers to output file (first iteration only)
            headers = Object.keys(record);
            outputStream.write(headers.join(',') + '\n');
        }

        // Process and write each data row
        const row = headers.map(header => record[header]);
        outputStream.write(row.join(',') + '\n');
    }

    outputStream.end();
    console.log('Processing completed. Output written to:', outputFilePath);
}

/**
 * Restore CSV data to PostgreSQL table using COPY command
 * Uses streaming for efficient handling of large files
 * 
 * @param {string} tableName - Name of the table to restore data to
 * @param {string} csvFilePath - Path to the processed CSV file
 */
async function restoreCsvToTable(tableName, csvFilePath) {
    const client = await pool.connect();
    try {
        // Start database transaction for data integrity
        await client.query('BEGIN');
        
        // Read CSV headers to construct dynamic column list
        const headers = await readCsvHeaders(csvFilePath);
 
        // Create a dynamic column list for the COPY command
        const columns = headers.join(',');
    
        // Use PostgreSQL's COPY command for efficient bulk data import
        const copyQuery = `\COPY ${tableName} (${columns}) FROM STDIN WITH DELIMITER ',' CSV HEADER;`;
        const fileStream = fs.createReadStream(csvFilePath);

        // Stream the CSV file directly to PostgreSQL using pg-copy-streams
        await new Promise((resolve, reject) => {
            const stream = client.query(from(copyQuery));

            fileStream.pipe(stream)
                .on('finish', resolve)
                .on('error', reject);
        });

        // Commit the transaction
        await client.query('COMMIT');
        console.log(`Data restored to table ${tableName} from ${csvFilePath}`);
    } catch (err) {
        // Rollback on error to maintain data integrity
        await client.query('ROLLBACK');
        console.error('Error during restore:', err);
        throw err;
    } finally {
        client.release();
    }
}

/**
 * Read CSV headers from file
 * Extracts column names from the first row of the CSV file
 * 
 * @param {string} filePath - Path to the CSV file
 * @returns {Promise<Array>} Array of column names
 */
async function readCsvHeaders(filePath) {
    return new Promise((resolve, reject) => {
        const inputStream = fs.createReadStream(filePath);

        const parser = inputStream.pipe(
            parse({
                columns: true,              // Treat first row as headers
                skip_empty_lines: true,     // Skip empty lines
                relax_column_count: true,   // Allow variable column count
            })
        );

        parser.once('data', (row) => {
            // Extract headers (keys of the first row)
            resolve(Object.keys(row));
            parser.destroy(); // Stop reading after getting the headers
        });
        parser.once('error', (err) => reject(err));
    });
}

/**
 * Convert a readable stream to a buffer
 * Used for handling S3 response streams
 * 
 * @param {ReadableStream} stream - The stream to convert
 * @returns {Promise<Buffer>} Buffer containing all stream data
 */
function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
        stream.on('error', reject);
    });
}

// ===================== SCRIPT EXECUTION =====================
// Execute the restoration process
(async () => {
    await downloadAndRestoreS3File(S3_BUCKET_NAME, s3Key, tableName);
})();
