# Database Setup Guide

## Initial Setup (One-time)

### 1. Create Neon Database

1. Go to [console.neon.tech](https://console.neon.tech)
2. Create a new project
3. Copy the connection string (looks like `postgresql://user:password@host/dbname`)

### 2. Set Environment Variable

Add to your `.env` file:
```
DATABASE_URL=postgresql://user:password@host/dbname
```

### 3. Create Tables

Run the schema to create all tables and indexes:

```bash
psql $DATABASE_URL < schema.sql
```

Or if using a psql connection directly:
```bash
psql -h your-host -U your-user -d your-database < schema.sql
```

## Migrate Existing Data

Once tables are created, run the migration script to import your existing CSV analysis results:

```bash
python3 migrate_to_postgres.py
```

This will:
- Read all `red_flag_analysis_*.csv` files in the current directory
- Create `regulatory_documents` entries for each regulation
- Create `analyses` entries with all analysis metadata
- Create `red_flags` entries for each individual flag
- Link `statute_references` if the CSV exists

## Test Connection

```bash
python3 << 'EOF'
import psycopg
import os
from dotenv import load_dotenv

load_dotenv()
try:
    conn = psycopg.connect(os.getenv("DATABASE_URL"))
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM regulatory_documents")
        count = cur.fetchone()[0]
    print(f"✓ Connected! Found {count} documents in database")
    conn.close()
except Exception as e:
    print(f"✗ Connection failed: {e}")
EOF
```

## Common Queries

### See all NY regulations with issues
```sql
SELECT * FROM latest_analyses
WHERE state = 'NY' AND type = 'regulation'
AND has_implementation_issues = TRUE
ORDER BY analyzed_at DESC;
```

### Find high-severity flags
```sql
SELECT
  d.title,
  rf.category,
  rf.severity,
  rf.explanation
FROM red_flags rf
JOIN analyses a ON rf.analysis_id = a.id
JOIN regulatory_documents d ON a.document_id = d.id
WHERE rf.severity >= 8 AND d.state = 'NY'
ORDER BY rf.severity DESC;
```

### Compare regulations vs policy documents
```sql
SELECT type, COUNT(*), AVG(max_severity) as avg_severity
FROM latest_analyses
WHERE state = 'NY'
GROUP BY type;
```

### Count flags by category
```sql
SELECT
  rf.category,
  COUNT(*) as count,
  AVG(rf.severity) as avg_severity
FROM red_flags rf
JOIN analyses a ON rf.analysis_id = a.id
JOIN regulatory_documents d ON a.document_id = d.id
WHERE d.state = 'NY' AND a.is_current = TRUE
GROUP BY rf.category
ORDER BY count DESC;
```

## Dependencies

Install required Python packages:

```bash
pip install psycopg pandas python-dotenv
```

## Notes

- The migration script uses `ON CONFLICT` to handle re-runs gracefully
- When re-analyzing a document, old analyses are marked `is_current = FALSE`
- The `latest_analyses` view always shows the most recent analysis for each document
- Statute references are linked by document ID, not by title match
