/**
 * PostgreSQL Database Backup and Deletion Script
 * 
 * This script performs the following operations:
 * 1. Connects to a PostgreSQL database
 * 2. Exports data from specified table in batches (month by month)
 * 3. Compresses the exported data as CSV files
 * 4. Uploads compressed files to AWS S3
 * 5. Deletes the backed-up data from the database
 * 
 * IMPORTANT: This script processes data in batches to avoid memory issues
 * and uses transactions to ensure data integrity.
 * 
 * WARNING: Batch counter resets on each script run, which can cause
 * data loss if script is interrupted and rerun. See README for details.
 */

import moment from 'moment';
import fs from 'fs';
import zlib from 'zlib';
import csvWriterPkg from 'csv-writer';
import { PutObjectCommand } from '@aws-sdk/client-s3';

// Import utility functions and configurations
import { BACKUP_PATH } from './util.js';
import { s3 } from './util.js';
import { pool } from './util.js';
import { S3_BUCKET_NAME } from './util.js';

const csvWriter = csvWriterPkg.createObjectCsvWriter;

// ===================== CONFIGURATION SECTION =====================
// IMPORTANT: Update these values before running the script

const tableName = '<table_name>';        // Replace with your actual table name
const BATCH_SIZE = 10000;                // Number of rows to process in each batch

// --------------SET THE START AND END DATE-----------------
const START_DATE = '2022-03-01';         // Customize start date (YYYY-MM-DD format)
const END_DATE = '2022-12-31';           // Customize end date (YYYY-MM-DD format)
// ---------------------------------------------------------

// ===================== LOGGING SETUP =====================
// Create a writable stream for the log file to track backup operations
const logFile = fs.createWriteStream(`${BACKUP_PATH}/${tableName}_backup_log.txt`, { flags: 'a' });

// Redirect console.log to both log file and console for comprehensive logging
console.log = (message) => {
    logFile.write(message + '\n');
    process.stdout.write(message + '\n'); // Also display in console for real-time monitoring
};

// ===================== DATABASE SCHEMA CONFIGURATION =====================
// Uncomment and modify the following if you need to connect to a specific schema
// (other than the default 'public' schema)
// pool.on('connect', async (client) => {
//   await client.query(`SET search_path TO your_schema_name`);
// });

/**
 * Main function to backup and delete rows from the specified table
 * Processes data month by month within the specified date range
 */
async function backupAndDeleteRows() {
    // Get initial row count for logging purposes
    let count = await getRowCount(tableName);
    console.log(`Processing table: ${tableName} - ${count} rows ${new Date()}`);
    
    // Initialize date range for processing
    let currentDate = new Date(START_DATE);
    const endDate = new Date(END_DATE);

    // Process each month in the specified date range
    while (currentDate <= endDate) {
        const year = currentDate.getFullYear();
        const month = currentDate.getMonth() + 1; // JavaScript months are 0-indexed
        
        // Process all data for the current month
        await processMonth(tableName, year, month);

        // Move to the next month
        currentDate.setMonth(currentDate.getMonth() + 1);
    }
    
    // Log final row count after processing
    let finalCount = await getRowCount(tableName);
    console.log(`Processed table: ${tableName} - ${finalCount} rows ${new Date()}\n\n\n`);
}

/**
 * Process all data for a specific month
 * 
 * CRITICAL WARNING: Batch counter starts from 1 for each month and resets
 * on each script run. If the script is interrupted and rerun, it will
 * overwrite existing S3 files, causing data loss.
 * 
 * @param {string} tableName - Name of the table to process
 * @param {number} year - Year to process
 * @param {number} month - Month to process (1-12)
 */
async function processMonth(tableName, year, month) {
    // Calculate date range for the current month
    const monthStart = `${year}-${month.toString().padStart(2, '0')}-01`;
    const nextMonth = new Date(year, month, 1); // Calculate next month
    const nextMonthStart = `${nextMonth.getFullYear()}-${(nextMonth.getMonth() + 1).toString().padStart(2, '0')}-01`;

    let totalDeleted = 0;
    let batchNumber = 1; // WARNING: This resets to 1 on each script run!
    console.log(monthStart, nextMonthStart);

    // Fetch table column headers dynamically to ensure CSV compatibility
    const headers = await fetchTableHeaders(tableName);

    // Process data in batches until no more rows are found for this month
    while (true) {
        const client = await pool.connect();
        try {
            // Start database transaction for data integrity
            await client.query('BEGIN');

            // Use CTE (Common Table Expression) to select and delete rows in batches
            // This ensures we only delete what we've successfully backed up
            const result = await client.query(
                `
                WITH cte AS (
                    SELECT id 
                    FROM ${tableName}
                    WHERE created_at >= $1 AND created_at < $2
                    ORDER BY created_at ASC
                    LIMIT $3
                )
                DELETE FROM ${tableName}
                WHERE id IN (SELECT id FROM cte)
                RETURNING *;
            `,
                [monthStart, nextMonthStart, BATCH_SIZE]
            );

            const rows = result.rows;
            if (rows.length === 0) break; // No more rows to process for this month

            // Create compressed CSV file and upload to S3
            const filePath = await writeToCompressedFile(rows, tableName, year, month, batchNumber, headers);
            const s3Key = `db_backup/${tableName}/${year}-${month.toString().padStart(2, '0')}/backup_${year}-${month.toString().padStart(2, '0')}_batch${batchNumber}.csv.gz`;
            await uploadToS3(filePath, s3Key);

            totalDeleted += rows.length;
            console.log(`Processed and deleted ${rows.length} rows for ${year}-${month}. Batch ${batchNumber} completed.`);

            // Increment batch number for next iteration
            batchNumber++;

            // Commit the transaction
            await client.query('COMMIT');
        } catch (err) {
            console.error('Error during month processing, rolling back...', err);
            await client.query('ROLLBACK');
            throw err;
        } finally {
            client.release();
        }
    }

    console.log(`Completed processing for ${year}-${month}. Total rows deleted: ${totalDeleted}`);
}

/**
 * Fetch table column headers dynamically from the database
 * This ensures the CSV file has the correct headers regardless of table structure
 * 
 * @param {string} tableName - Name of the table to fetch headers for
 * @returns {Array} Array of header objects for CSV writer
 */
async function fetchTableHeaders(tableName) {
    const client = await pool.connect();
    try {
        const result = await client.query(
            `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position;
        `,
            [tableName]
        );
        
        // Note: ordinal_position ensures columns are returned in the same order
        // as they were defined in the table schema (1, 2, 3, etc.)

        // Convert column names to CSV writer format
        return result.rows.map((row) => ({ 
            id: row.column_name, 
            title: row.column_name.toUpperCase() 
        }));
    } finally {
        client.release();
    }
}

/**
 * Write database rows to a compressed CSV file
 * Handles data serialization, CSV creation, compression, and cleanup
 * 
 * @param {Array} rows - Database rows to write
 * @param {string} tableName - Name of the table
 * @param {number} year - Year for file naming
 * @param {number} month - Month for file naming
 * @param {number} batchNumber - Batch number for file naming
 * @param {Array} headers - CSV headers
 * @returns {string} Path to the compressed file
 */
async function writeToCompressedFile(rows, tableName, year, month, batchNumber, headers) {
    // Serialize rows to handle special data types (dates, JSON objects)
    const serializedRows = rows.map((row) => {
        const newRow = { ...row };
    
        // Handle special data types for CSV compatibility
        for (const key in newRow) {
            if (newRow[key] && typeof newRow[key] === 'object') {
                if (newRow[key] instanceof Date) {
                    // Format dates in ISO format for consistency
                    newRow[key] = moment(newRow[key]).format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');
                } else {
                    // Properly escape JSON objects for CSV
                    newRow[key] = `"${JSON.stringify(newRow[key]).replace(/"/g, '""')}"`;
                }
            }
        }
        return newRow;
    });

    // Generate file path with naming convention: tableName_YYYY-MM_batchN.csv
    const filePath = `${BACKUP_PATH}/${tableName}_${year}-${month.toString().padStart(2, '0')}_batch${batchNumber}.csv`;
    const csv = csvWriter({ 
        path: filePath, 
        header: headers, 
        alwaysQuote: true, 
        fieldDelimiter: ',' 
    });

    // Write data to CSV file
    await csv.writeRecords(serializedRows);

    // Validate the CSV content before compression
    const fileContents = fs.readFileSync(filePath, 'utf-8');
    if (!fileContents || fileContents.split('\n').length < 2) {
        throw new Error('CSV file is empty or malformed.');
    }
    console.log(`CSV generated: ${filePath}`);

    // Compress the CSV file using gzip
    const compressedPath = `${filePath}.gz`;
    const compressed = zlib.gzipSync(fileContents);
    fs.writeFileSync(compressedPath, compressed);
    console.log(`Compressed file generated: ${compressedPath}`);

    // Clean up: remove the original CSV file to save disk space
    fs.unlinkSync(filePath);

    // Verify compressed file was created successfully
    if (!fs.existsSync(compressedPath)) {
        throw new Error('File generation or compression failed.');
    }

    return compressedPath;
}

/**
 * Get the total number of rows in the specified table
 * Used for logging and monitoring purposes
 * 
 * @param {string} tableName - Name of the table to count rows for
 * @returns {number} Total number of rows in the table
 */
async function getRowCount(tableName) {
    const client = await pool.connect();
    try {
        const result = await client.query(`SELECT COUNT(*) AS count FROM ${tableName}`);
        return parseInt(result.rows[0].count, 10);
    } catch (err) {
        console.error('Error getting row count:', err);
        throw err;
    } finally {
        client.release();
    }
}

/**
 * Upload a file to AWS S3 bucket
 * 
 * @param {string} filePath - Local path to the file to upload
 * @param {string} s3Key - S3 key (path) where the file will be stored
 */
async function uploadToS3(filePath, s3Key) {
    const fileContents = fs.readFileSync(filePath);
    const params = {
        Bucket: S3_BUCKET_NAME,
        Key: s3Key,
        Body: fileContents,
    };
    const command = new PutObjectCommand(params);

    try {
        const data = await s3.send(command);
        console.log(`Uploaded file to S3: ${s3Key}`);
    } catch (err) {
        console.error('Error uploading file to S3:', err);
        throw err;
    }
}

// ===================== SCRIPT EXECUTION =====================
console.log('Starting backup and deletion process...', new Date());
backupAndDeleteRows();
