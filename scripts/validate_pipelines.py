#!/usr/bin/env python3
import json
import os
import re
import requests
import sys
import time
from pathlib import Path
from deepdiff import DeepDiff

def wait_for_elasticsearch(request_kwargs, elasticsearch_url="http://localhost:9200", timeout=120):
    """Wait for Elasticsearch to be ready and healthy"""
    print("Waiting for Elasticsearch to start...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(elasticsearch_url, **request_kwargs)
            if response.status_code in [200, 401]:
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
                **request_kwargs
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

def simulate_pipeline(request_kwargs, pipeline_data, docs_data, elasticsearch_url="http://localhost:9200"):
    """Send pipeline simulation request to Elasticsearch"""
    payload = {"pipeline": pipeline_data, **docs_data}

    try:
        response = requests.post(
            f"{elasticsearch_url}/_ingest/pipeline/_simulate",
            json=payload,
            headers={"Content-Type": "application/json"},
            **request_kwargs
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as e:
        print("Elasticsearch returned an HTTP error")

        try:
            error_details = e.response.json()
            print(json.dumps(error_details, indent=2))
        except json.JSONDecodeError:
            print(f"Raw error response: {e.resonse.text}")

        raise Exception(f"Elasticsearch simulation failed with status code {e.response.status_code}")

    except Exception as e:
        raise Exception(f"An unexpected error occurred during simulation: {e}")

def get_pipelines_to_test():
    """
    Determine which pipeline directories to test.
    This is driven by the PIPELINES_TO_TEST environment variable, which
    contains a space-separated list of directories.
    """
    pipelines_str = os.environ.get('PIPELINES_TO_TEST', '').strip()


    if not pipelines_str:
        print("No pipelines to test. Skipping validation.")
        return []

    return pipelines_str.split()

from deepdiff import DeepDiff

def main():
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD")
    request_kwargs = {
            "timeout": 10
    }

    if not es_password:
        print("Warning: ES_PASSWORD not set, trying unauthenticated request.")
        auth_params = None
    else:
        request_kwargs['auth'] = (es_user, es_password)
    event_name = os.environ.get('GITHUB_EVENT_NAME')
    if event_name == 'workflow_dispatch':
        debug_enabled_str = os.environ.get('DEBUG_ENABLED', 'false')
        debug_on = debug_enabled_str.lower() == 'true'
        print(f"Manual run detected. Visual diff: {'Enabled' if debug_on else 'Disabled'}")
    else:
        debug_on = True # Always enable for automated CI runs
    debug_on = True

    wait_for_elasticsearch(request_kwargs=request_kwargs)
    
    print("Starting pipeline validation...")
    pipelines = get_pipelines_to_test()
    if not pipelines:
        return 0
    
    print(f"Found {len(pipelines)} pipeline director(y/ies) to test...")
    print()
    
    tests_run = 0
    tests_passed = 0
    tests_failed = 0
    

    for pipeline_dir in pipelines:
        # If locally running, add ../ to path
        if not os.getenv('GITHUB_ACTIONS'):
            script_dir = Path(__file__).resolve().parent
            pipeline_path = script_dir.parent / "pipelines" / pipeline_dir
        else:
            pipeline_path = Path(pipeline_dir)
        pipeline_file = pipeline_path / "pipeline.json"
        example_file = pipeline_path / "simulate_example.json"
        results_file = pipeline_path / "simulate_results.json"
        
        if not all(f.exists() for f in [pipeline_file, example_file, results_file]):
            print(f"---\n[INFO] Skipping directory {pipeline_dir} (missing required .json files)")
            continue
        
        print(f"---\n[TEST] Testing pipeline in directory: {pipeline_dir}")
        tests_run += 1
        
        try:
            pipeline_data = load_and_fix_pipeline(pipeline_file)
            example_data = load_json_file(example_file)
            expected_result = load_json_file(results_file)
            actual_result = simulate_pipeline(request_kwargs, pipeline_data, example_data)
            normalized_actual = normalize_result(actual_result)
            normalized_expected = normalize_result(expected_result)

             
            json_diff = DeepDiff(normalized_expected, normalized_actual, ignore_order=True)
            
            if not json_diff:
                print("[SUCCESS] Simulation result matches expected result.")
                tests_passed += 1
                if debug_on:
                    print(f"::group::Visual Diff for {pipeline_dir}")
                    print("--- VISUAL DIFF (Expected vs. Actual) ---")
                    print(json_diff.pretty())
                    print("-----------------------------------------")
                    print("::endgroup::")
            else:
                tests_failed += 1
                print("[FAILURE] Simulation result does not match expected result.")
                print(f"::group::Visual Diff for {pipeline_dir}")
                print("--- VISUAL DIFF (Expected vs. Actual) ---")
                print(json_diff.pretty())
                print("-----------------------------------------")
                print("::endgroup::")
        
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
