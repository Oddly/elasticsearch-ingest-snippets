#!/usr/bin/env python3
import json
import os
import re
import requests
import sys
import time
from pathlib import Path

def wait_for_elasticsearch(elasticsearch_url="http://localhost:9200", timeout=120):
    """Wait for Elasticsearch to be ready and healthy"""
    print("Waiting for Elasticsearch to start...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(elasticsearch_url, timeout=5)
            if response.status_code == 200:
                print("Elasticsearch is responding!")
                break
        except requests.exceptions.RequestException:
            pass
        print(".", end="", flush=True)
        time.sleep(2)
    else:
        raise Exception(f"Elasticsearch did not start within {timeout} seconds")
    
    print("Waiting for Elasticsearch cluster to be healthy...")
    start_time = time.time()
    while time.time() - start_time < 60:
        try:
            health_response = requests.get(
                f"{elasticsearch_url}/_cluster/health?wait_for_status=yellow&timeout=10s",
                timeout=15
            )
            if health_response.status_code == 200:
                health_data = health_response.json()
                if health_data.get('status') in ['green', 'yellow']:
                    print(f"Elasticsearch cluster is healthy! (status: {health_data.get('status')})")
                    return
        except requests.exceptions.RequestException:
            pass
        print(".", end="", flush=True)
        time.sleep(2)
    else:
        print("Warning: Cluster health check timed out, proceeding anyway")

def fix_triple_quotes(content):
    """Convert triple-quoted strings to properly escaped JSON strings"""
    def replace_triple_quote(match):
        inner_content = match.group(1)
        escaped = json.dumps(inner_content)[1:-1]
        return '"' + escaped + '"'
    
    pattern = r'"""(.*?)"""'
    return re.sub(pattern, replace_triple_quote, content, flags=re.DOTALL)

def load_and_fix_pipeline(pipeline_file):
    """Load a pipeline file and fix any triple-quote issues"""
    try:
        with open(pipeline_file, 'r') as f:
            content = f.read()
        fixed_content = fix_triple_quotes(content)
        return json.loads(fixed_content)
    except Exception as e:
        raise Exception(f"Failed to load pipeline from {pipeline_file}: {e}")

def load_json_file(filepath):
    """Load and validate a JSON file"""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise Exception(f"Failed to load JSON from {filepath}: {e}")

def normalize_result(result):
    """Remove timestamp fields for deterministic comparison"""
    if isinstance(result, dict):
        return {k: normalize_result(v) for k, v in result.items() if k != 'timestamp'}
    elif isinstance(result, list):
        return [normalize_result(item) for item in result]
    return result

def simulate_pipeline(pipeline_data, docs_data, elasticsearch_url="http://localhost:9200"):
    """Send pipeline simulation request to Elasticsearch"""
    payload = {"pipeline": pipeline_data, "docs": docs_data}
    try:
        response = requests.post(
            f"{elasticsearch_url}/_ingest/pipeline/_simulate",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise Exception(f"Elasticsearch simulation failed: {e}")

def get_pipelines_to_test():
    """Determine which pipeline directories to test based on Git changes."""
    changed_files_str = os.environ.get('CHANGED_FILES', '').strip()
    
    if not changed_files_str:
        print("No changed files detected in 'pipelines' directory. Skipping validation.")
        return []
    
    # Extract unique parent directories from the list of changed files
    changed_files = changed_files_str.split()
    pipeline_dirs = set()
    for file_path in changed_files:
        path = Path(file_path)
        # Assuming structure is pipelines/<pipeline_name>/...
        if path.parts and path.parts[0] == 'pipelines' and len(path.parts) > 1:
            pipeline_dirs.add(str(Path(path.parts[0]) / path.parts[1]))
            
    return sorted(list(pipeline_dirs))

def main():
    wait_for_elasticsearch()
    
    print("Starting pipeline validation...")
    
    pipelines = get_pipelines_to_test()
    if not pipelines:
        return 0
    
    print(f"Found {len(pipelines)} changed pipeline director(y/ies) to test...")
    print()
    
    tests_run = 0
    tests_passed = 0
    tests_failed = 0
    
    for pipeline_dir in pipelines:
        pipeline_path = Path(pipeline_dir)
        pipeline_file = pipeline_path / "pipeline.json"
        example_file = pipeline_path / "simulate_example.json"
        results_file = pipeline_path / "simulate_results.json"
        
        if not all(f.exists() for f in [pipeline_file, example_file, results_file]):
            print("---\n[INFO] Skipping directory {pipeline_dir} (missing required .json files)")
            continue
        
        print(f"---\n[TEST] Testing pipeline in directory: {pipeline_dir}")
        tests_run += 1
        
        try:
            pipeline_data = load_and_fix_pipeline(pipeline_file)
            example_data = load_json_file(example_file)
            expected_result = load_json_file(results_file)
            
            actual_result = simulate_pipeline(pipeline_data, example_data['docs'])
            
            normalized_actual = normalize_result(actual_result)
            normalized_expected = normalize_result(expected_result)
            
            if normalized_actual == normalized_expected:
                print("[SUCCESS] Simulation result matches expected result.")
                tests_passed += 1
            else:
                print("[FAILURE] Simulation result does not match expected result.")
                print("---------- EXPECTED ----------")
                print(json.dumps(normalized_expected, indent=2, sort_keys=True))
                print("----------- ACTUAL -----------")
                print(json.dumps(normalized_actual, indent=2, sort_keys=True))
                print("----------------------------")
                tests_failed += 1
        except Exception as e:
            print(f"[ERROR] An exception occurred during the test: {e}")
            tests_failed += 1
    
    print("\n--- Test Summary ---")
    print(f"Tests run:    {tests_run}")
    print(f"Passed:     {tests_passed}")
    print(f"Failed:     {tests_failed}")
    
    if tests_failed > 0:
        print("\nOne or more pipeline tests failed.")
        return 1
    else:
        print("\nAll pipeline tests passed!")
        return 0

if __name__ == "__main__":
    sys.exit(main())
