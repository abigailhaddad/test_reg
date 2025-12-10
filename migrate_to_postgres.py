#!/usr/bin/env python3
"""
Migration script to move existing red flag analysis results into PostgreSQL
"""

import json
import pandas as pd
import psycopg
from pathlib import Path
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Create connection to Neon PostgreSQL database"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not set in environment variables")

    # Neon connection strings use psycopg (PostgreSQL driver)
    return psycopg.connect(database_url)

def migrate_analyses(csv_file: str, state: str = "NY", doc_type: str = "regulation"):
    """
    Migrate analysis results from CSV to PostgreSQL

    Args:
        csv_file: Path to CSV file with analysis results
        state: State code (e.g., 'NY', 'CA')
        doc_type: Document type ('regulation' or 'policy_document')
    """

    print(f"Reading {csv_file}...")
    df = pd.read_csv(csv_file)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for idx, row in df.iterrows():
                try:
                    # Insert regulatory document
                    cur.execute("""
                        INSERT INTO regulatory_documents
                        (source_index, type, state, title, url, url_type, content)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (state, source_index, type) DO UPDATE SET
                            title = EXCLUDED.title,
                            url = EXCLUDED.url,
                            content = EXCLUDED.content
                        RETURNING id
                    """, (
                        int(row['source_index']),
                        doc_type,
                        state,
                        row['title'],
                        row['url'],
                        row['url_type'],
                        row['content']
                    ))

                    doc_id = cur.fetchone()[0]

                    # Mark previous analyses as not current
                    cur.execute("""
                        UPDATE analyses SET is_current = FALSE
                        WHERE document_id = %s
                    """, (doc_id,))

                    # Insert analysis
                    cur.execute("""
                        INSERT INTO analyses
                        (document_id, model_version, has_implementation_issues,
                         overall_complexity, summary, requires_technical_review,
                         has_reporting_requirement, max_severity, num_flags)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        doc_id,
                        "gpt-5-nano-2025-08-07",
                        bool(row['has_implementation_issues']),
                        row['overall_complexity'] if pd.notna(row['overall_complexity']) else None,
                        row['summary'] if pd.notna(row['summary']) else None,
                        bool(row['requires_technical_review']),
                        bool(row['has_reporting_requirement']),
                        int(row['max_severity']) if pd.notna(row['max_severity']) else 0,
                        int(row['num_flags']) if pd.notna(row['num_flags']) else 0
                    ))

                    analysis_id = cur.fetchone()[0]

                    # Parse and insert red flags
                    red_flags_json = row['red_flags']
                    if pd.notna(red_flags_json) and red_flags_json != '[]':
                        flags = json.loads(red_flags_json)
                        for flag in flags:
                            cur.execute("""
                                INSERT INTO red_flags
                                (analysis_id, category, explanation, severity, complexity,
                                 matched_phrases, implementation_approach, effort_estimate,
                                 text_examples)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                analysis_id,
                                flag['category'],
                                flag['explanation'],
                                flag['severity'],
                                flag['complexity'],
                                flag.get('matched_phrases', []),
                                flag['implementation_approach'],
                                flag.get('effort_estimate'),
                                flag.get('text_examples', [])
                            ))

                    if (idx + 1) % 10 == 0:
                        print(f"  ✓ Migrated {idx + 1} documents...")
                        conn.commit()

                except Exception as e:
                    print(f"  ✗ Error migrating row {idx} ({row['title'][:50]}): {e}")
                    conn.rollback()
                    raise

        conn.commit()

    print(f"✓ Successfully migrated {len(df)} analyses from {csv_file}")

def migrate_statute_references(csv_file: str, state: str = "NY"):
    """
    Migrate federal statute references to PostgreSQL

    Args:
        csv_file: Path to CSV file with statute references
        state: State code
    """

    print(f"Reading statute references from {csv_file}...")
    df = pd.read_csv(csv_file)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for idx, row in df.iterrows():
                try:
                    # Find the document by title (assuming titles are unique per state)
                    cur.execute("""
                        SELECT id FROM regulatory_documents
                        WHERE state = %s AND title = %s
                        LIMIT 1
                    """, (state, row['title']))

                    result = cur.fetchone()
                    if not result:
                        print(f"  ⚠ Document not found for: {row['title'][:50]}")
                        continue

                    doc_id = result[0]

                    # Insert statute reference
                    cur.execute("""
                        INSERT INTO statute_references
                        (document_id, usc_citations, cfr_citations, public_laws, acts,
                         state_title, state_section)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (document_id) DO UPDATE SET
                            usc_citations = EXCLUDED.usc_citations,
                            cfr_citations = EXCLUDED.cfr_citations,
                            public_laws = EXCLUDED.public_laws,
                            acts = EXCLUDED.acts
                    """, (
                        doc_id,
                        row['usc_citations'] if pd.notna(row['usc_citations']) else None,
                        row['cfr_citations'] if pd.notna(row['cfr_citations']) else None,
                        row['public_laws'] if pd.notna(row['public_laws']) else None,
                        row['acts'] if pd.notna(row['acts']) else None,
                        row['ny_title'] if pd.notna(row['ny_title']) else None,
                        row['ny_section'] if pd.notna(row['ny_section']) else None
                    ))

                    if (idx + 1) % 10 == 0:
                        print(f"  ✓ Migrated {idx + 1} statute references...")
                        conn.commit()

                except Exception as e:
                    print(f"  ✗ Error migrating statute reference {idx}: {e}")
                    conn.rollback()
                    raise

        conn.commit()

    print(f"✓ Successfully migrated statute references from {csv_file}")

def main():
    print("Starting migration to PostgreSQL...\n")

    # Find all analysis CSV files
    analysis_files = list(Path(".").glob("red_flag_analysis_*.csv"))

    if not analysis_files:
        print("No red_flag_analysis_*.csv files found!")
        return

    # Migrate each analysis CSV
    for csv_file in sorted(analysis_files):
        try:
            migrate_analyses(str(csv_file), state="NY", doc_type="regulation")
        except Exception as e:
            print(f"Failed to migrate {csv_file}: {e}")
            raise

    # Migrate statute references if file exists
    statute_file = Path("federal_statute_references.csv")
    if statute_file.exists():
        try:
            migrate_statute_references(str(statute_file), state="NY")
        except Exception as e:
            print(f"Failed to migrate statute references: {e}")
            # Don't fail the whole migration if statute refs fail
            print("Continuing despite statute reference migration failure...")

    print("\n✓ Migration complete!")

if __name__ == "__main__":
    main()
