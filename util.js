/**
 * Utility Configuration File
 * 
 * This file contains shared configurations and connections used by both
 * backup and restore scripts. It handles:
 * - AWS S3 client configuration
 * - PostgreSQL database connection pool
 * - Environment variable management
 * - Common constants and paths
 */

import { S3Client } from '@aws-sdk/client-s3';
import pkg from 'pg';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

const { Pool } = pkg;

// ===================== AWS S3 CONFIGURATION =====================
/**
 * AWS S3 Client instance
 * Configured with credentials from environment variables
 */
export const s3 = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    }
});

// ===================== POSTGRESQL CONFIGURATION =====================
/**
 * PostgreSQL connection pool
 * Manages database connections efficiently with connection reuse
 */
export const pool = new Pool({
    user: process.env.POSTGRES_USER,
    host: process.env.POSTGRES_HOST,
    database: process.env.POSTGRES_DATABASE,
    password: process.env.POSTGRES_PASSWORD,
    port: process.env.POSTGRES_PORT
});

// ===================== CONSTANTS AND PATHS =====================
/**
 * S3 bucket name for storing backup files
 */
export const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME;

/**
 * Local directory path for temporary backup files and logs
 * This directory will be created automatically if it doesn't exist
 */
export const BACKUP_PATH = 'backup_logs';

