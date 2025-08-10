#!/usr/bin/env python3
# /// script
# dependencies = [
#     "requests",
#     "deepdiff",
#     "rich"
# ]
# ///

import json
import os
import re
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

import requests
from deepdiff import DeepDiff
from rich.console import Console
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, TextColumn,
                           TimeRemainingColumn)
from rich.table import Table

# --- Configuration ---
console = Console(highlight=False)
RequestKwargs = Dict[str, Any]
PERF_TEST_ITERATIONS = 100
# --- End Configuration ---


def log_github_error(message: str, file_path: Path):
    if os.getenv("GITHUB_ACTIONS") == "true":
        workspace = os.getenv("GITHUB_WORKSPACE", ".")
        relative_path = file_path.relative_to(workspace) if workspace in file_path.parents else file_path
        console.print(f"::error file={relative_path}::{message}")


def wait_for_elasticsearch(request_kwargs: RequestKwargs, elasticsearch_url: str = "http://localhost:9200", timeout: int = 120):
    console.print("Waiting for Elasticsearch to start...")
    with console.status("[yellow]Pinging Elasticsearch...", spinner="dots"):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(elasticsearch_url, **request_kwargs)
                if response.status_code in [200, 401]:
                    console.print("[green][✓][/green] Elasticsearch is responding!")
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        else:
            raise ConnectionTimeoutError(f"Elasticsearch did not start at {elasticsearch_url} within {timeout} seconds")

    console.print("Waiting for Elasticsearch cluster to be healthy...")
    with console.status("[yellow]Checking cluster health...", spinner="dots"):
        start_time = time.time()
        while time.time() - start_time < 60:
            try:
                health_url = f"{elasticsearch_url}/_cluster/health?wait_for_status=yellow&timeout=10s"
                health_response = requests.get(health_url, **request_kwargs)
                health_response.raise_for_status()
                health_data = health_response.json()
                if health_data.get('status') in ['green', 'yellow']:
                    console.print(f"[green][✓][/green] Elasticsearch cluster is healthy! (status: {health_data.get('status')})")
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        else:
            console.print("\n[yellow][WARNING][/yellow] Cluster health check timed out, proceeding anyway")


def fix_triple_quotes(content: str) -> str:
    return re.sub(r'"""(.*?)"""', lambda m: json.dumps(m.group(1)), content, flags=re.DOTALL)


def load_and_fix_pipeline(pipeline_file: Path) -> Dict[str, Any]:
    try:
        content = pipeline_file.read_text(encoding="utf-8")
        return json.loads(fix_triple_quotes(content))
    except (json.JSONDecodeError, Exception) as e:
        raise ValueError(f"Failed to load or parse pipeline from '{pipeline_file}'") from e


def load_json_file(filepath: Path) -> Dict[str, Any]:
    try:
        return json.loads(filepath.read_text(encoding="utf-8"))
    except (json.JSONDecodeError) as e:
        raise ValueError(f"Failed to load or parse JSON from '{filepath}'") from e


def normalize_result(result: Any) -> Any:
    if isinstance(result, dict):
        keys_to_exclude = {'_ingest', '_index', '_id', '_version'}
        return {key: normalize_result(value) for key, value in result.items() if key not in keys_to_exclude}
    if isinstance(result, list):
        return [normalize_result(item) for item in result]
    return result


def simulate_pipeline(
    request_kwargs: RequestKwargs, pipeline_data: Dict[str, Any], example_data: Dict[str, Any], elasticsearch_url: str = "http://localhost:9200"
) -> Dict[str, Any]:
    payload = {"pipeline": pipeline_data, **example_data}
    simulate_url = f"{elasticsearch_url}/_ingest/pipeline/_simulate"
    try:
        response = requests.post(simulate_url, json=payload, **request_kwargs)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        error_details = e.response.json() if "json" in e.response.headers.get("content-type", "") else e.response.text
        raise ValueError(f"Simulation failed with status {e.response.status_code}", error_details) from e
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Could not connect to Elasticsearch at {simulate_url}") from e


def get_pipelines_to_test() -> List[str]:
    pipelines_str = os.environ.get('PIPELINES_TO_TEST', '').strip()
    return pipelines_str.split() if pipelines_str else []


def run_tests_for_pipeline(pipeline_dir_name: str, pipelines_base_dir: Path, request_kwargs: RequestKwargs) -> dict:
    console.print(f"\n[bold]Running Tests for Pipeline:[/bold] [bold magenta]{pipeline_dir_name}[/bold magenta]\n")
    start_time = time.monotonic()
    stats = {"passed": 0, "failed": 0, "failed_names": []}
    pipeline_ok = True
    pipeline_path = pipelines_base_dir / pipeline_dir_name
    pipeline_file = pipeline_path / "pipeline.json"

    if not pipeline_file.exists():
        console.print(f"[yellow][SKIP][/yellow] Directory '{pipeline_dir_name}' skipped (missing pipeline.json)")
        stats["failed"] = 1
        stats["failed_names"].append(f"{pipeline_dir_name} (Setup/Missing pipeline.json)")
        stats['duration'] = time.monotonic() - start_time
        return stats

    try:
        pipeline_data = load_and_fix_pipeline(pipeline_file)
    except (ValueError, FileNotFoundError) as e:
        log_github_error(f"Failed to load main pipeline file: {e}", pipeline_file)
        console.print(Panel(f"[bold]Fatal error loading pipeline.[/bold]\n\nDetails: {e}", title="[red]Setup Failure", border_style="red"))
        stats["failed"] = 1
        stats["failed_names"].append(f"{pipeline_dir_name} (Setup/Load Failure)")
        stats['duration'] = time.monotonic() - start_time
        return stats

    static_example_files = sorted(list(pipeline_path.glob("simulate_example_*.json")))
    if static_example_files:
        passed_count = 0
        try:
            with console.status("[yellow]Running Static Tests...", spinner="dots"):
                for example_file in static_example_files:
                    test_case_name = example_file.stem.replace("simulate_example_", "")
                    results_file = example_file.with_name(f"simulate_results_{test_case_name}.json")
                    if not results_file.exists():
                        raise FileNotFoundError(f"Missing corresponding results file: {results_file.name}")
                    
                    example_data = load_json_file(example_file)
                    expected_result = load_json_file(results_file)
                    actual_result = simulate_pipeline(request_kwargs, pipeline_data, example_data)
                    diff = DeepDiff(normalize_result(expected_result), normalize_result(actual_result), ignore_order=True)

                    if diff:
                        raise AssertionError(f"Test case '{test_case_name}' failed.", diff.pretty())
                    passed_count += 1
            console.print(f"[green][✓][/green] Static Tests ({passed_count}/{len(static_example_files)} passed)")
            stats["passed"] += passed_count
        except (ValueError, FileNotFoundError, ConnectionError, AssertionError) as e:
            pipeline_ok = False
            stats["failed"] += 1
            failure_name = f"{pipeline_dir_name} / static / {e.args[0] if e.args else 'Unknown'}"
            stats["failed_names"].append(failure_name)
            log_github_error(str(e.args[0]), pipeline_file)
            console.print(f"[red][✗][/red] Static Tests")
            details = f"\n\n[bold]Diff:[/bold]\n{e.args[1]}" if isinstance(e, AssertionError) else str(e)
            console.print(Panel(f"[bold]Failure during static test execution.[/bold]\n{details}", title="[red]Test Failure", border_style="red"))
            
    happy_path_doc = None
    if pipeline_ok:
        happy_path_files = sorted(list(pipeline_path.glob("simulate_example_happy_path_*.json")))
        if happy_path_files:
            happy_path_doc = load_json_file(happy_path_files[0])
            source_doc = happy_path_doc.get("docs", [{}])[0].get("_source", {})
            primary_input_fields = list(source_doc.keys())
            
            test_generators = [
                ({"docs": [{"_source": {}}]}, "Empty Document"),
                ({"docs": [{"_source": {"foo": "bar"}}]}, "Unrelated Fields"),
            ]
            doc_with_extra = deepcopy(happy_path_doc)
            doc_with_extra["docs"][0]["_source"]["zz_test_field"] = "preserved"
            test_generators.append((doc_with_extra, "Extra Field Preservation"))
            for field in primary_input_fields:
                test_generators.append( ({"docs": [{"_source": {field: None}}]}, f"Null Value for '{field}'") )

            with Progress(TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TimeRemainingColumn()) as progress:
                task_id = progress.add_task("[yellow]Running Dynamic Tests...", total=len(test_generators))
                for input_doc, test_name in test_generators:
                    progress.update(task_id, advance=1)
                    try:
                        expected_doc = simulate_pipeline(request_kwargs, pipeline_data, input_doc)
                        actual_result = simulate_pipeline(request_kwargs, pipeline_data, input_doc)
                        diff = DeepDiff(normalize_result(expected_doc), normalize_result(actual_result), ignore_order=True)
                        if diff: raise AssertionError(f"Test '{test_name}' failed.", diff.pretty())
                        stats["passed"] += 1
                    except (ValueError, ConnectionError, AssertionError) as e:
                        pipeline_ok = False
                        stats["failed"] += 1
                        failure_name = f"{pipeline_dir_name} / dynamic / {test_name}"
                        stats["failed_names"].append(failure_name)
                        progress.update(task_id, description=f"[red][✗][/red] Dynamic Tests")
                        log_github_error(str(e.args[0]), pipeline_file)
                        details = f"\n\n[bold]Diff:[/bold]\n{e.args[1]}" if isinstance(e, AssertionError) else str(e)
                        console.print(Panel(f"[bold]Dynamic test '{test_name}' failed.[/bold]\nDetails: {details}", title="[red]Test Failure", border_style="red"))
                        break
                if pipeline_ok:
                    progress.update(task_id, description=f"[green][✓][/green] Dynamic Tests ({len(test_generators)} passed)")
        else:
             console.print("[yellow][SKIP][/yellow] No 'happy_path' file found. Skipping dynamic & performance tests.")

    if pipeline_ok and happy_path_doc:
        try:
            with console.status("[yellow]Running Performance Test...", spinner="dots"):
                perf_start_time = time.monotonic()
                for _ in range(PERF_TEST_ITERATIONS):
                    simulate_pipeline(request_kwargs, pipeline_data, happy_path_doc)
                perf_duration = time.monotonic() - perf_start_time
            
            docs_per_second = PERF_TEST_ITERATIONS / perf_duration
            avg_time_per_doc_ms = (perf_duration / PERF_TEST_ITERATIONS) * 1000
            console.print("[green][✓][/green] Performance Test")
            console.print(f"      [dim]↳ Avg. Time/Document: {avg_time_per_doc_ms:.2f} ms | Throughput: {docs_per_second:.2f} docs/sec[/dim]")
        except (ValueError, ConnectionError) as e:
            console.print(f"[red][✗][/red] Performance Test")
            console.print(Panel(f"Performance test runner failed with exception:\n{e}", title="[red]Fatal Error", border_style="red"))

    stats['duration'] = time.monotonic() - start_time
    return stats


def main() -> int:
    overall_start_time = time.monotonic()
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD")
    request_kwargs: RequestKwargs = {"timeout": 10}
    if es_password:
        request_kwargs['auth'] = (es_user, es_password)

    try:
        wait_for_elasticsearch(request_kwargs=request_kwargs)
    except ConnectionTimeoutError as e:
        console.print(f"[red][FATAL][/red] {e}")
        return 1

    console.print("\n[bold]Starting pipeline validation...[/bold]")

    pipelines_to_run = get_pipelines_to_test()
    repo_root = Path(__file__).resolve().parent.parent if not os.getenv('GITHUB_ACTIONS') else Path(".")
    pipelines_base_dir = repo_root / "pipelines"

    if pipelines_to_run:
        console.print(f"Testing specified subset of pipelines: [bold]{', '.join(pipelines_to_run)}[/bold]")
    else:
        console.print("[yellow]PIPELINES_TO_TEST not set. Discovering all available pipelines...[/yellow]")
        try:
            pipelines_to_run = sorted([d.name for d in pipelines_base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')])
            if not pipelines_to_run:
                console.print(f"[red]No pipeline directories found in {pipelines_base_dir}. Nothing to do.[/red]")
                return 0
            console.print(f"Found and will test the following: [bold]{', '.join(pipelines_to_run)}[/bold]")
        except FileNotFoundError:
            console.print(f"[red]Error: The directory {pipelines_base_dir} does not exist. Exiting.[/red]")
            return 1
    
    # Enrich results with pipeline name for detailed summary
    all_results = []
    for pipeline_name in pipelines_to_run:
        result_stats = run_tests_for_pipeline(pipeline_name, pipelines_base_dir, request_kwargs)
        result_stats['name'] = pipeline_name
        all_results.append(result_stats)
    
    # --- Final Summary ---
    total_passed = sum(r['passed'] for r in all_results)
    total_failed = sum(r['failed'] for r in all_results)
    failed_tests_summary = [name for r in all_results for name in r['failed_names']]
    total_duration = time.monotonic() - overall_start_time

    console.print("\n--- [bold]Final Test Summary[/bold] ---")
    
    summary_table = Table(
        title="Per-Pipeline Results",
        show_header=True,
        header_style="bold cyan",
        show_footer=True,
        footer_style="bold"
    )
    summary_table.add_column("Pipeline", footer="Overall Totals", no_wrap=True)
    summary_table.add_column("Status", justify="center")
    summary_table.add_column("Passed", justify="right", footer=f"[green]{total_passed}[/green]")
    summary_table.add_column("Failed", justify="right", footer=f"[red]{total_failed}[/red]" if total_failed else "0")
    summary_table.add_column("Duration (s)", justify="right", footer=f"{total_duration:.2f}s")
    
    for result in all_results:
        status = "[red]FAILED[/red]" if result['failed'] > 0 else "[green]PASSED[/green]"
        summary_table.add_row(
            result['name'],
            status,
            str(result['passed']),
            str(result['failed']),
            f"{result['duration']:.2f}s"
        )
    
    console.print(summary_table)

    if total_failed > 0:
        console.print("\n[bold red]Failed Test Details:[/bold red]")
        for failure in failed_tests_summary:
            console.print(f" - {failure}")
        return 1
    else:
        console.print("\n[bold green]ALL TESTS PASSED[/bold green]")
        return 0

if __name__ == "__main__":
    sys.exit(main())
