import os
import json
import asyncio
import httpx
from pathlib import Path
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000"  # Change to your actual server URL
ARCHIVE_PATH = "../Archive (6)"
RESULTS_PATH = "../results"

async def send_compliance_request(session: httpx.AsyncClient, data: dict, file_path: str):
    """Send compliance request to the endpoint"""
    try:
        payload = {
            "data": data,
            "request_id": f"test_{datetime.now().isoformat()}"
        }
        
        response = await session.post(
            f"{BASE_URL}/compliance/check",
            json=payload,
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()
    
    except Exception as e:
        return {
            "error": str(e),
            "file_path": file_path,
            "timestamp": datetime.now().isoformat()
        }

def find_fb_flat_files(archive_path: str):
    """Find all fb_flat.json files in the archive directory"""
    fb_flat_files = []
    
    for root, dirs, files in os.walk(archive_path):
        for file in files:
            if file == "fb_flat.json":
                fb_flat_files.append(os.path.join(root, file))
    
    return fb_flat_files

async def process_files():
    """Process all fb_flat.json files and save results"""
    print(f"Looking for fb_flat.json files in: {ARCHIVE_PATH}")
    
    # Find all fb_flat.json files
    fb_flat_files = find_fb_flat_files(ARCHIVE_PATH)
    print(f"Found {len(fb_flat_files)} fb_flat.json files")
    
    if not fb_flat_files:
        print("No fb_flat.json files found!")
        return
    
    # Create results directory if it doesn't exist
    os.makedirs(RESULTS_PATH, exist_ok=True)
    
    async with httpx.AsyncClient() as session:
        results = []
        
        for i, file_path in enumerate(fb_flat_files, 1):
            print(f"Processing file {i}/{len(fb_flat_files)}: {file_path}")
            
            try:
                # Read the JSON file
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Send request to compliance endpoint
                response = await send_compliance_request(session, data, file_path)
                
                # Prepare result
                result = {
                    "file_path": file_path,
                    "folder_name": os.path.basename(os.path.dirname(file_path)),
                    "timestamp": datetime.now().isoformat(),
                    "input_data": data,
                    "response": response
                }
                
                results.append(result)
                
                # Save individual result
                folder_name = os.path.basename(os.path.dirname(file_path))
                result_filename = f"result_{folder_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                result_path = os.path.join(RESULTS_PATH, result_filename)
                
                with open(result_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                
                print(f"✓ Processed and saved: {result_filename}")
                
                # Add small delay to avoid overwhelming the server
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"✗ Error processing {file_path}: {str(e)}")
                error_result = {
                    "file_path": file_path,
                    "folder_name": os.path.basename(os.path.dirname(file_path)),
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e),
                    "input_data": None,
                    "response": None
                }
                results.append(error_result)
        
        # Save summary results
        summary_path = os.path.join(RESULTS_PATH, f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        summary = {
            "total_files": len(fb_flat_files),
            "processed_successfully": len([r for r in results if "error" not in r or not r.get("error")]),
            "failed": len([r for r in results if "error" in r and r.get("error")]),
            "timestamp": datetime.now().isoformat(),
            "results": results
        }
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n🎉 Processing completed!")
        print(f"📊 Summary:")
        print(f"  - Total files: {summary['total_files']}")
        print(f"  - Successfully processed: {summary['processed_successfully']}")
        print(f"  - Failed: {summary['failed']}")
        print(f"  - Results saved in: {RESULTS_PATH}")
        print(f"  - Summary file: {summary_path}")

def main():
    """Main function"""
    print("🚀 Starting bulk compliance testing...")
    print(f"Archive path: {ARCHIVE_PATH}")
    print(f"Results path: {RESULTS_PATH}")
    print(f"Endpoint: {BASE_URL}/compliance/check")
    print("-" * 50)
    
    # Check if archive directory exists
    if not os.path.exists(ARCHIVE_PATH):
        print(f"❌ Archive directory not found: {ARCHIVE_PATH}")
        return
    
    # Run the async processing
    asyncio.run(process_files())

if __name__ == "__main__":
    main()