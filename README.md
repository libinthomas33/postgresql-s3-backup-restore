# PostgreSQL Database Backup and Restore Scripts

A comprehensive solution for backing up PostgreSQL table data to AWS S3 and restoring it when needed. Efficiently handles large datasets by processing data in batches and storing compressed backups in the cloud.

## 🚨 CRITICAL WARNING - Data Loss Risk

**⚠️ BATCH COUNTER RESET ISSUE**: The batch counter resets to 1 every time you run the backup script. If you interrupt the script and rerun it, **it will overwrite existing S3 files with the same batch numbers, causing permanent data loss**.

**Example:**
1. Run script → Creates `backup_2025-05_batch1.csv.gz`, `backup_2025-05_batch2.csv.gz`, etc.
2. Script interrupted after processing 50,000 rows (5 batches)
3. Rerun script → **Overwrites** `backup_2025-05_batch1.csv.gz` (data loss!)

**⚠️ RECOMMENDATION**: Always run the backup script to completion without interruption.

## Quick Start

**1. Install and configure**
```bash
git clone https://github.com/your-username/postgresql-s3-backup-restore.git

cd postgresql-s3-backup-restore

npm install
```

**2. Setup environment**
```bash
cp .env.sample .env
```
Edit `.env` with your AWS and PostgreSQL credentials.

**3. Configure and run backup**
Edit `delete_backup.js` and set: `tableName`, `START_DATE`, `END_DATE`
```bash
npm run backup
```

**4. Configure and run restore** *(when needed)*
Edit `restore.js` and set: `tableName`, `yearMonth`, `batchNo`
```bash
npm run restore
```

## How It Works

### Backup Process (`delete_backup.js`)
1. **Data Export**: Exports table rows in monthly batches to CSV files
2. **Compression**: Compresses CSV files using gzip
3. **S3 Upload**: Uploads to organized S3 folder structure
4. **Data Deletion**: Safely deletes backed-up rows from database
5. **Logging**: Comprehensive logging to console and files

### Restore Process (`restore.js`)
1. **S3 Download**: Downloads specific backup files from S3
2. **Decompression**: Decompresses gzipped files
3. **Data Processing**: Processes CSV for PostgreSQL compatibility
4. **Database Import**: Uses PostgreSQL's COPY command for efficient import

## Configuration

### Environment Variables (.env)

```env
# AWS Configuration
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
S3_BUCKET_NAME=your-backup-bucket

# PostgreSQL Configuration
POSTGRES_USER=your_db_user
POSTGRES_HOST=localhost
POSTGRES_DATABASE=your_database_name
POSTGRES_PASSWORD=your_db_password
POSTGRES_PORT=5432
```

### Script Configuration

**Backup Script (`delete_backup.js`)**:
```javascript
const tableName = 'your_table_name';        // Replace with actual table name
const BATCH_SIZE = 10000;                   // Rows per batch
const START_DATE = '2022-03-01';            // Start date (YYYY-MM-DD)
const END_DATE = '2022-12-31';              // End date (YYYY-MM-DD)
```

**Restore Script (`restore.js`)**:
```javascript
const tableName = 'your_table_name';        // Same table name as backup
const yearMonth = '2022-03';                // Year-month to restore (YYYY-MM)
const batchNo = 1;                          // Specific batch number to restore
```

**Custom Schema** (if not using `public` schema):
```javascript
pool.on('connect', async (client) => {
  await client.query(`SET search_path TO your_schema_name`);
});
```

## Prerequisites

- **Node.js** (version 14 or later)
- **PostgreSQL** database with appropriate permissions
- **AWS Account** with S3 access
- **Table with `created_at` column** for date-based filtering

## File Structure

```
├── delete_backup.js     # Main backup script
├── restore.js          # Main restore script
├── util.js             # Shared utilities and configurations
├── package.json        # Node.js dependencies
├── .env.sample         # Environment variables template
├── backup_logs/        # Local temporary files and logs
└── README.md          # This documentation
```

## S3 Organization

```
your-s3-bucket/
└── db_backup/
    └── {table_name}/
        ├── 2022-03/
        │   ├── backup_2022-03_batch1.csv.gz
        │   ├── backup_2022-03_batch2.csv.gz
        │   └── ...
        └── 2022-04/
            └── ...
```

## Performance Tuning

- **Default batch size**: 10,000 rows
- **Large tables**: Reduce to 5,000 for better memory management
- **Small tables**: Increase to 50,000 for faster processing
- **Run during off-peak hours** to minimize database impact
- **Monitor disk space** for temporary CSV files

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Table not found** | Verify table name and schema |
| **AWS credentials error** | Check `.env` file and S3 permissions |
| **Database connection error** | Verify PostgreSQL parameters |
| **Out of memory** | Reduce `BATCH_SIZE` value |
| **S3 upload failures** | Check connectivity and bucket access |
| **Duplicate key errors** | Delete existing data or use different table |

2. **AWS credentials error**:
   - Verify `.env` file configuration
   - Ensure AWS credentials have S3 permissions

### Monitoring
- **Backup logs**: `backup_logs/{table_name}_backup_log.txt`
- **Console output**: Real-time progress
- **S3 verification**: Check bucket for successful uploads

## Security Best Practices

- Never commit `.env` file to version control
- Use IAM roles with minimal S3 permissions
- Use dedicated backup database user
- Configure S3 bucket encryption and policies

## Contributing

1. Follow existing code style and commenting patterns
2. Test with different data types and sizes
3. Update documentation for new features
4. Consider the batch counter reset issue in modifications

## License

MIT License - Open source and free to use.

---

**⚠️ Remember**: Always run backup scripts to completion to avoid data loss due to batch counter resets!