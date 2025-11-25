#!/usr/bin/env python3
"""
Fix individual file creation to ensure 1:1 mapping with master file
"""

import json
import re
from pathlib import Path
import hashlib

def create_safe_filename(url, index):
    """Create a guaranteed unique, safe filename"""
    # Extract meaningful part of URL
    url_part = url.split('/')[-1] if '/' in url else url
    
    # Clean up the URL part for filename
    clean_name = re.sub(r'[^\w\-\.]', '_', url_part)
    
    # Remove multiple underscores and clean up
    clean_name = re.sub(r'_+', '_', clean_name)
    clean_name = clean_name.strip('_')
    
    # Limit length and add index for uniqueness
    if len(clean_name) > 100:
        clean_name = clean_name[:100]
    
    # Add index to guarantee uniqueness
    filename = f"{index:06d}_{clean_name}.json"
    
    return filename

def main():
    # Load the master file
    print("📄 Loading master regulations file...")
    with open('ny_regulations_from_cache.json', 'r', encoding='utf-8') as f:
        regulations = json.load(f)
    
    print(f"📊 Master file contains: {len(regulations)} regulations")
    
    # Create fresh output directory
    output_dir = Path('individual_regulations_fixed')
    if output_dir.exists():
        print("🗑️  Removing old individual files...")
        import shutil
        shutil.rmtree(output_dir)
    output_dir.mkdir()
    
    print("📝 Creating individual files with guaranteed unique names...")
    
    created_count = 0
    failed_count = 0
    filename_conflicts = {}
    
    for i, regulation in enumerate(regulations):
        try:
            url = regulation['url']
            
            # Create guaranteed unique filename using index
            filename = create_safe_filename(url, i)
            
            # Check for conflicts (shouldn't happen with index, but let's be sure)
            if filename in filename_conflicts:
                # Add hash if somehow there's still a conflict
                url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
                filename = f"{i:06d}_{url_hash}.json"
            
            filename_conflicts[filename] = url
            
            # Create individual file with cleaned content
            individual_file = {
                'url': regulation['url'],
                'title': regulation['title'],
                'content': regulation['cleaned_content'],  # Use cleaned version!
                'url_type': regulation.get('url_type', 'unknown'),
                'source_index': i  # Track original position
            }
            
            file_path = output_dir / filename
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(individual_file, f, indent=2, ensure_ascii=False)
            
            created_count += 1
            
            if created_count % 1000 == 0:
                print(f"  Created {created_count}/{len(regulations)} files...")
                
        except Exception as e:
            print(f"❌ Failed to create file for regulation {i}: {e}")
            print(f"   URL: {regulation.get('url', 'unknown')}")
            failed_count += 1
            continue
    
    print(f"\n✅ Results:")
    print(f"  📄 Master file: {len(regulations)} regulations")
    print(f"  📁 Individual files created: {created_count}")
    print(f"  ❌ Failed: {failed_count}")
    print(f"  📍 Location: {output_dir}/")
    
    if created_count == len(regulations):
        print("🎉 Perfect! 1:1 mapping achieved!")
    else:
        print(f"⚠️  Missing {len(regulations) - created_count} files")
    
    # Verify by counting files
    actual_files = len(list(output_dir.glob('*.json')))
    print(f"  🔍 Verification: {actual_files} files on disk")
    
    return created_count == len(regulations)

if __name__ == "__main__":
    main()