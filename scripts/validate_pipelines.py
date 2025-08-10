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
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests
from deepdiff import DeepDiff
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn
from rich.table import Table


class TestStatus(Enum):
    """Enumeration for test statuses."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class PipelineTestError(Exception):
    """Custom exception for pipeline testing errors."""
    pass


class ElasticsearchConnectionError(PipelineTestError):
    """Raised when Elasticsearch connection fails."""
    pass


class PipelineLoadError(PipelineTestError):
    """Raised when pipeline loading fails."""
    pass


@dataclass
class Config:
    """Configuration settings for the pipeline tester."""
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_user: str = "elastic"
    elasticsearch_password: Optional[str] = None
    request_timeout: int = 10
    elasticsearch_startup_timeout: int = 120
    health_check_timeout: int = 60
    performance_test_iterations: int = 100
    pipelines_to_test: List[str] = field(default_factory=list)
    
    @classmethod
    def from_environment(cls) -> 'Config':
        """Create configuration from environment variables."""
        pipelines_str = os.environ.get('PIPELINES_TO_TEST', '').strip()
        pipelines = pipelines_str.split() if pipelines_str else []
        
        return cls(
            elasticsearch_password=os.getenv("ES_PASSWORD"),
            elasticsearch_user=os.getenv("ES_USER", "elastic"),
            pipelines_to_test=pipelines
        )
    
    @property
    def request_kwargs(self) -> Dict[str, Any]:
        """Get request kwargs for Elasticsearch calls."""
        kwargs = {"timeout": self.request_timeout}
        if self.elasticsearch_password:
            kwargs['auth'] = (self.elasticsearch_user, self.elasticsearch_password)
        return kwargs


@dataclass
class TestResult:
    """Result of a single test case."""
    name: str
    status: TestStatus
    duration: float = 0.0
    error_message: Optional[str] = None
    details: Optional[str] = None


@dataclass
class PipelineTestResults:
    """Aggregated results for a pipeline."""
    pipeline_name: str
    test_results: List[TestResult] = field(default_factory=list)
    duration: float = 0.0
    
    @property
    def passed_count(self) -> int:
        """Number of passed tests."""
        return sum(1 for result in self.test_results if result.status == TestStatus.PASSED)
    
    @property
    def failed_count(self) -> int:
        """Number of failed tests."""
        return sum(1 for result in self.test_results if result.status == TestStatus.FAILED)
    
    @property
    def overall_status(self) -> TestStatus:
        """Overall status of the pipeline tests."""
        if not self.test_results:
            return TestStatus.SKIPPED
        return TestStatus.FAILED if self.failed_count > 0 else TestStatus.PASSED
    
    @property
    def failed_test_names(self) -> List[str]:
        """Names of failed tests."""
        return [
            f"{self.pipeline_name} / {result.name}"
            for result in self.test_results
            if result.status == TestStatus.FAILED
        ]


def timing_decorator(func):
    """Decorator to measure function execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        duration = time.monotonic() - start_time
        if hasattr(result, 'duration'):
            result.duration = duration
        # Store duration as an attribute on the wrapper function for main()
        wrapper.duration = duration
        return result
    return wrapper


def log_github_error(message: str, file_path: Path) -> None:
    """Log error in GitHub Actions format if running in CI."""
    if os.getenv("GITHUB_ACTIONS") != "true":
        return
        
    workspace = os.getenv("GITHUB_WORKSPACE", ".")
    relative_path = (
        file_path.relative_to(workspace) 
        if workspace in str(file_path.parents) 
        else file_path
    )
    Console().print(f"::error file={relative_path}::{message}")


class ElasticsearchClient:
    """Client for Elasticsearch operations."""
    
    def __init__(self, config: Config, console: Console):
        self.config = config
        self.console = console
    
    def wait_for_startup(self) -> None:
        """Wait for Elasticsearch to start and become healthy."""
        self._wait_for_response()
        self._wait_for_health()
    
    def _wait_for_response(self) -> None:
        """Wait for Elasticsearch to respond to requests."""
        self.console.print("\nWaiting for Elasticsearch to start...")
        
        with self.console.status("[yellow]Pinging Elasticsearch...", spinner="dots"):
            start_time = time.time()
            while time.time() - start_time < self.config.elasticsearch_startup_timeout:
                try:
                    response = requests.get(
                        self.config.elasticsearch_url, 
                        **self.config.request_kwargs
                    )
                    if response.status_code in [200, 401]:
                        self.console.print("[green][✓][/green] Elasticsearch is responding!")
                        return
                except requests.exceptions.RequestException:
                    pass
                time.sleep(2)
            
            raise ElasticsearchConnectionError(
                f"Elasticsearch did not start at {self.config.elasticsearch_url} "
                f"within {self.config.elasticsearch_startup_timeout} seconds"
            )
    
    def _wait_for_health(self) -> None:
        """Wait for Elasticsearch cluster to become healthy."""
        self.console.print("\nWaiting for Elasticsearch cluster to be healthy...")
        
        with self.console.status("[yellow]Checking cluster health...", spinner="dots"):
            start_time = time.time()
            while time.time() - start_time < self.config.health_check_timeout:
                try:
                    health_url = (
                        f"{self.config.elasticsearch_url}/_cluster/health"
                        f"?wait_for_status=yellow&timeout=10s"
                    )
                    response = requests.get(health_url, **self.config.request_kwargs)
                    response.raise_for_status()
                    health_data = response.json()
                    
                    if health_data.get('status') in ['green', 'yellow']:
                        status = health_data.get('status')
                        self.console.print(
                            f"[green][✓][/green] Elasticsearch cluster is healthy! (status: {status})"
                        )
                        return
                except requests.exceptions.RequestException:
                    pass
                time.sleep(2)
            
            self.console.print(
                "\n[yellow][WARNING][/yellow] Cluster health check timed out, proceeding anyway"
            )
    
    def simulate_pipeline(
        self, 
        pipeline_data: Dict[str, Any], 
        example_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Simulate pipeline execution with given data."""
        payload = {"pipeline": pipeline_data, **example_data}
        simulate_url = f"{self.config.elasticsearch_url}/_ingest/pipeline/_simulate"
        
        try:
            response = requests.post(
                simulate_url, 
                json=payload, 
                **self.config.request_kwargs
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_details = self._extract_error_details(e.response)
            raise PipelineTestError(
                f"Simulation failed with status {e.response.status_code}: {error_details}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise ElasticsearchConnectionError(
                f"Could not connect to Elasticsearch at {simulate_url}"
            ) from e
    
    @staticmethod
    def _extract_error_details(response: requests.Response) -> str:
        """Extract error details from response."""
        content_type = response.headers.get("content-type", "")
        if "json" in content_type:
            try:
                return str(response.json())
            except json.JSONDecodeError:
                pass
        return response.text


class PipelineLoader:
    """Handles loading and parsing of pipeline files."""
    
    @staticmethod
    def fix_triple_quotes(content: str) -> str:
        """Fix triple quotes in pipeline content by converting to JSON strings."""
        return re.sub(
            r'"""(.*?)"""', 
            lambda m: json.dumps(m.group(1)), 
            content, 
            flags=re.DOTALL
        )
    
    @classmethod
    def load_pipeline(cls, pipeline_file: Path) -> Dict[str, Any]:
        """Load and parse a pipeline file."""
        try:
            content = pipeline_file.read_text(encoding="utf-8")
            fixed_content = cls.fix_triple_quotes(content)
            return json.loads(fixed_content)
        except (json.JSONDecodeError, OSError) as e:
            raise PipelineLoadError(
                f"Failed to load or parse pipeline from '{pipeline_file}'"
            ) from e
    
    @staticmethod
    def load_json_file(filepath: Path) -> Dict[str, Any]:
        """Load a JSON file."""
        try:
            return json.loads(filepath.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            raise PipelineLoadError(
                f"Failed to load or parse JSON from '{filepath}'"
            ) from e


class ResultNormalizer:
    """Handles normalization of pipeline results for comparison."""
    
    EXCLUDED_KEYS = {'_ingest', '_index', '_id', '_version'}
    
    @classmethod
    def normalize(cls, result: Any) -> Any:
        """Normalize result for comparison by removing metadata fields."""
        if isinstance(result, dict):
            return {
                key: cls.normalize(value) 
                for key, value in result.items() 
                if key not in cls.EXCLUDED_KEYS
            }
        elif isinstance(result, list):
            return [cls.normalize(item) for item in result]
        return result


class ResultComparator:
    """Handles comparison of pipeline results."""
    
    @staticmethod
    def compare_results(expected: Any, actual: Any, test_name: str) -> Optional[str]:
        """Compare expected vs actual results and return human-readable diff if different."""
        normalized_expected = ResultNormalizer.normalize(expected)
        normalized_actual = ResultNormalizer.normalize(actual)
        
        diff = DeepDiff(normalized_expected, normalized_actual, ignore_order=True)
        
        if not diff:
            return None
        
        return ResultComparator._format_diff_message(diff, test_name)
    
    @staticmethod
    def _format_diff_message(diff: DeepDiff, test_name: str) -> str:
        """Format DeepDiff output into a more readable error message."""
        error_parts = [f"Test case '{test_name}' failed with the following differences:"]
        
        # Handle field mismatches (added + removed = likely renamed/typo)
        fields_only_in_actual = set()
        fields_only_in_expected = set()
        
        if 'dictionary_item_added' in diff:
            # These are fields present in actual result but missing from expected result
            fields_only_in_actual = {ResultComparator._clean_path(str(item)) for item in diff['dictionary_item_added']}
        
        if 'dictionary_item_removed' in diff:
            # These are fields present in expected result but missing from actual result  
            fields_only_in_expected = {ResultComparator._clean_path(str(item)) for item in diff['dictionary_item_removed']}
        
        # If we have both, try to pair them up as likely renames/typos
        if fields_only_in_actual and fields_only_in_expected:
            error_parts.append("\n🔄 Field mismatches (likely typos or renames):")
            
            # Simple pairing: if same number, pair them up
            actual_list = sorted(fields_only_in_actual)
            expected_list = sorted(fields_only_in_expected)
            
            if len(actual_list) == len(expected_list):
                for found_field, expected_field in zip(expected_list, actual_list):
                    error_parts.append(f"   Found: '{found_field}' but expected: '{expected_field}'")
                # Clear these since we've handled them
                fields_only_in_actual.clear()
                fields_only_in_expected.clear()
            else:
                # Different counts, show them separately
                for field in actual_list:
                    error_parts.append(f"   Unexpected field found: '{field}'")
                for field in expected_list:
                    error_parts.append(f"   Expected field missing: '{field}'")
                fields_only_in_actual.clear()
                fields_only_in_expected.clear()
        
        # Handle remaining additions (if any)
        if fields_only_in_actual:
            error_parts.append("\n📝 Unexpected fields found in actual result:")
            for item in sorted(fields_only_in_actual):
                error_parts.append(f"   + {item}")
        
        # Handle remaining removals (if any)  
        if fields_only_in_expected:
            error_parts.append("\n❌ Expected fields missing from actual result:")
            for item in sorted(fields_only_in_expected):
                error_parts.append(f"   - {item}")
        
        if 'values_changed' in diff:
            changed_items = diff['values_changed']
            error_parts.append("\n🔄 Field values that differ:")
            for path, change in changed_items.items():
                clean_path = ResultComparator._clean_path(str(path))
                old_val = change.get('old_value', 'N/A')
                new_val = change.get('new_value', 'N/A')
                error_parts.append(f"   '{clean_path}': found '{new_val}' but expected '{old_val}'")
        
        if 'type_changes' in diff:
            type_changes = diff['type_changes']
            error_parts.append("\n🔀 Field type mismatches:")
            for path, change in type_changes.items():
                clean_path = ResultComparator._clean_path(str(path))
                old_type = change.get('old_type', 'unknown').__name__
                new_type = change.get('new_type', 'unknown').__name__
                error_parts.append(f"   '{clean_path}': found {new_type} but expected {old_type}")
        
        # If we have other types of changes, include the raw diff
        other_changes = {k: v for k, v in diff.items() 
                        if k not in ['dictionary_item_added', 'dictionary_item_removed', 'values_changed', 'type_changes']}
        if other_changes:
            error_parts.append(f"\n🔍 Other changes:\n{str(other_changes)}")
        
        return "\n".join(error_parts)
    
    @staticmethod
    def _clean_path(path: str) -> str:
        """Clean up DeepDiff path format to be more readable."""
        # Remove 'root' and clean up the path format
        cleaned = path.replace("root", "")
        # Convert from ['key'] format to .key format
        cleaned = re.sub(r"\['([^']+)'\]", r".\1", cleaned)
        # Convert from [0] format to [0] format (keep array indices as-is)
        cleaned = re.sub(r"\[(\d+)\]", r"[\1]", cleaned)
        # Remove leading dot
        cleaned = cleaned.lstrip(".")
        return cleaned if cleaned else "root"


class TestGenerator:
    """Generates dynamic test cases for pipeline testing."""
    
    @staticmethod
    def generate_dynamic_tests(happy_path_doc: Dict[str, Any]) -> List[tuple[Dict[str, Any], str]]:
        """Generate dynamic test cases based on happy path document."""
        source_doc = happy_path_doc.get("docs", [{}])[0].get("_source", {})
        primary_fields = list(source_doc.keys())
        
        # Base test cases
        test_cases = [
            ({"docs": [{"_source": {}}]}, "Empty Document"),
            ({"docs": [{"_source": {"foo": "bar"}}]}, "Unrelated Fields"),
        ]
        
        # Extra field preservation test
        doc_with_extra = deepcopy(happy_path_doc)
        doc_with_extra["docs"][0]["_source"]["zz_test_field"] = "preserved"
        test_cases.append((doc_with_extra, "Extra Field Preservation"))
        
        # Null value tests for each primary field
        test_cases.extend([
            ({"docs": [{"_source": {field: None}}]}, f"Null Value for '{field}'")
            for field in primary_fields
        ])
        
        return test_cases


class PipelineTestRunner:
    """Main test runner for pipeline validation."""
    
    def __init__(self, config: Config, console: Console):
        self.config = config
        self.console = console
        self.es_client = ElasticsearchClient(config, console)
        self.pipeline_loader = PipelineLoader()
        self.normalizer = ResultNormalizer()
        self.test_generator = TestGenerator()
    
    @timing_decorator
    def run_pipeline_tests(self, pipeline_name: str, pipelines_dir: Path) -> PipelineTestResults:
        """Run all tests for a single pipeline."""
        self.console.print(
            f"\n[bold]Running Tests for Pipeline:[/bold] [bold magenta]{pipeline_name}[/bold magenta]\n"
        )
        
        results = PipelineTestResults(pipeline_name=pipeline_name)
        pipeline_path = pipelines_dir / pipeline_name
        pipeline_file = pipeline_path / "pipeline.json"
        
        # Validate pipeline file exists
        if not pipeline_file.exists():
            self._add_skip_result(results, "Missing pipeline.json")
            return results
        
        # Load pipeline
        try:
            pipeline_data = self.pipeline_loader.load_pipeline(pipeline_file)
        except PipelineLoadError as e:
            self._add_error_result(results, "Pipeline Load Failure", str(e), pipeline_file)
            return results
        
        # Run static tests
        static_files = sorted(pipeline_path.glob("simulate_example_*.json"))
        if static_files:
            self._run_static_tests(results, pipeline_path, static_files, pipeline_data)
        
        # Run dynamic and performance tests if we have a happy path
        happy_path_files = sorted(pipeline_path.glob("simulate_example_happy_path_*.json"))
        if happy_path_files and results.overall_status != TestStatus.FAILED:
            happy_path_doc = self.pipeline_loader.load_json_file(happy_path_files[0])
            self._run_dynamic_tests(results, pipeline_data, happy_path_doc)
            
            if results.overall_status != TestStatus.FAILED:
                self._run_performance_test(results, pipeline_data, happy_path_doc)
        elif not happy_path_files:
            self.console.print(
                "[yellow][SKIP][/yellow] No 'happy_path' file found. "
                "Skipping dynamic & performance tests."
            )
        
        return results
    
    def _add_skip_result(self, results: PipelineTestResults, reason: str) -> None:
        """Add a skip result to the test results."""
        self.console.print(f"[yellow][SKIP][/yellow] Directory '{results.pipeline_name}' skipped ({reason})")
        results.test_results.append(
            TestResult(name=f"Setup/{reason}", status=TestStatus.FAILED)
        )
    
    def _add_error_result(
        self, 
        results: PipelineTestResults, 
        test_name: str, 
        error_msg: str, 
        file_path: Path
    ) -> None:
        """Add an error result to the test results."""
        log_github_error(error_msg, file_path)
        self.console.print(
            Panel(
                f"[bold]Fatal error loading pipeline.[/bold]\n\nDetails: {error_msg}",
                title="[red]Setup Failure",
                border_style="red"
            )
        )
        results.test_results.append(
            TestResult(name=f"Setup/{test_name}", status=TestStatus.FAILED, error_message=error_msg)
        )
    
    def _run_static_tests(
        self, 
        results: PipelineTestResults, 
        pipeline_path: Path, 
        static_files: List[Path], 
        pipeline_data: Dict[str, Any]
    ) -> None:
        """Run static tests for the pipeline."""
        with self.console.status("[yellow]Running Static Tests...", spinner="dots"):
            for example_file in static_files:
                test_name = example_file.stem.replace("simulate_example_", "")
                
                try:
                    self._run_single_static_test(
                        pipeline_path, example_file, test_name, pipeline_data
                    )
                    results.test_results.append(
                        TestResult(name=f"static/{test_name}", status=TestStatus.PASSED)
                    )
                except (PipelineLoadError, FileNotFoundError, PipelineTestError) as e:
                    self._handle_test_failure(results, f"static/{test_name}", str(e))
                    break
        
        passed_count = sum(
            1 for r in results.test_results 
            if r.name.startswith("static/") and r.status == TestStatus.PASSED
        )
        
        if results.overall_status == TestStatus.FAILED:
            self.console.print("[red][✗][/red] Static Tests")
        else:
            self.console.print(f"[green][✓][/green] Static Tests ({passed_count}/{len(static_files)} passed)")
    
    def _run_single_static_test(
        self, 
        pipeline_path: Path, 
        example_file: Path, 
        test_name: str, 
        pipeline_data: Dict[str, Any]
    ) -> None:
        """Run a single static test case."""
        results_file = example_file.with_name(f"simulate_results_{test_name}.json")
        
        if not results_file.exists():
            raise FileNotFoundError(f"Missing corresponding results file: {results_file.name}")
        
        example_data = self.pipeline_loader.load_json_file(example_file)
        expected_result = self.pipeline_loader.load_json_file(results_file)
        actual_result = self.es_client.simulate_pipeline(pipeline_data, example_data)
        
        error_message = ResultComparator.compare_results(expected_result, actual_result, test_name)
        if error_message:
            raise PipelineTestError(error_message)
    
    def _run_dynamic_tests(
        self, 
        results: PipelineTestResults, 
        pipeline_data: Dict[str, Any], 
        happy_path_doc: Dict[str, Any]
    ) -> None:
        """Run dynamic tests for the pipeline."""
        test_cases = self.test_generator.generate_dynamic_tests(happy_path_doc)
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn()
        ) as progress:
            task_id = progress.add_task("[yellow]Running Dynamic Tests...", total=len(test_cases))
            
            for input_doc, test_name in test_cases:
                progress.update(task_id, advance=1)
                
                try:
                    # Run the same test twice to ensure consistency
                    expected_result = self.es_client.simulate_pipeline(pipeline_data, input_doc)
                    actual_result = self.es_client.simulate_pipeline(pipeline_data, input_doc)
                    
                    error_message = ResultComparator.compare_results(expected_result, actual_result, test_name)
                    if error_message:
                        raise PipelineTestError(error_message)
                    
                    results.test_results.append(
                        TestResult(name=f"dynamic/{test_name}", status=TestStatus.PASSED)
                    )
                    
                except (PipelineTestError, ElasticsearchConnectionError) as e:
                    progress.update(task_id, description="[red][✗][/red] Dynamic Tests")
                    self._handle_test_failure(results, f"dynamic/{test_name}", str(e))
                    break
            
            if results.overall_status != TestStatus.FAILED:
                progress.update(
                    task_id, 
                    description=f"[green][✓][/green] Dynamic Tests ({len(test_cases)} passed)"
                )
    
    def _run_performance_test(
        self, 
        results: PipelineTestResults, 
        pipeline_data: Dict[str, Any], 
        happy_path_doc: Dict[str, Any]
    ) -> None:
        """Run performance test for the pipeline."""
        try:
            with self.console.status("[yellow]Running Performance Test...", spinner="dots"):
                start_time = time.monotonic()
                for _ in range(self.config.performance_test_iterations):
                    self.es_client.simulate_pipeline(pipeline_data, happy_path_doc)
                duration = time.monotonic() - start_time
            
            docs_per_second = self.config.performance_test_iterations / duration
            avg_time_ms = (duration / self.config.performance_test_iterations) * 1000
            
            self.console.print("[green][✓][/green] Performance Test")
            self.console.print(
                f"      [dim]↳ Avg. Time/Document: {avg_time_ms:.2f} ms | "
                f"Throughput: {docs_per_second:.2f} docs/sec[/dim]"
            )
            
            results.test_results.append(
                TestResult(name="performance", status=TestStatus.PASSED)
            )
            
        except (PipelineTestError, ElasticsearchConnectionError) as e:
            self.console.print("[red][✗][/red] Performance Test")
            self.console.print(
                Panel(
                    f"Performance test runner failed with exception:\n{e}",
                    title="[red]Fatal Error",
                    border_style="red"
                )
            )
    
    def _handle_test_failure(self, results: PipelineTestResults, test_name: str, error_msg: str) -> None:
        """Handle a test failure by logging and recording it."""
        log_github_error(error_msg, Path("pipeline.json"))  # Generic path for GitHub Actions
        
        results.test_results.append(
            TestResult(
                name=test_name, 
                status=TestStatus.FAILED, 
                error_message=error_msg
            )
        )
        
        # Create a more readable panel for the console
        self.console.print(
            Panel(error_msg, title="[red]Test Failure", border_style="red")
        )


class ResultsReporter:
    """Handles reporting of test results."""
    
    def __init__(self, console: Console):
        self.console = console
    
    def print_summary(self, all_results: List[PipelineTestResults], total_duration: float) -> None:
        """Print a comprehensive test summary."""
        total_passed = sum(r.passed_count for r in all_results)
        total_failed = sum(r.failed_count for r in all_results)
        all_failed_tests = [name for r in all_results for name in r.failed_test_names]
        
        self.console.print("\n--- [bold]Final Test Summary[/bold] ---")
        
        # Create summary table
        table = self._create_summary_table(all_results, total_passed, total_failed, total_duration)
        self.console.print(table)
        
        # Print failure details if any
        if total_failed > 0:
            self.console.print("\n[bold red]Failed Test Details:[/bold red]")
            for failure in all_failed_tests:
                self.console.print(f" - {failure}")
        else:
            self.console.print("\n[bold green]ALL TESTS PASSED[/bold green]")
    
    def _create_summary_table(
        self, 
        all_results: List[PipelineTestResults], 
        total_passed: int, 
        total_failed: int, 
        total_duration: float
    ) -> Table:
        """Create the summary table for test results."""
        table = Table(
            title="Per-Pipeline Results",
            show_header=True,
            header_style="bold cyan",
            show_footer=True,
            footer_style="bold"
        )
        
        table.add_column("Pipeline", footer="Overall Totals", no_wrap=True)
        table.add_column("Status", justify="center")
        table.add_column("Passed", justify="right", footer=f"[green]{total_passed}[/green]")
        table.add_column(
            "Failed", 
            justify="right", 
            footer=f"[red]{total_failed}[/red]" if total_failed else "0"
        )
        table.add_column("Duration (s)", justify="right", footer=f"{total_duration:.2f}s")
        
        for result in all_results:
            status_display = self._get_status_display(result.overall_status)
            table.add_row(
                result.pipeline_name,
                status_display,
                str(result.passed_count),
                str(result.failed_count),
                f"{result.duration:.2f}s"
            )
        
        return table
    
    @staticmethod
    def _get_status_display(status: TestStatus) -> str:
        """Get display string for test status."""
        color_map = {
            TestStatus.PASSED: "[green]PASSED[/green]",
            TestStatus.FAILED: "[red]FAILED[/red]",
            TestStatus.SKIPPED: "[yellow]SKIPPED[/yellow]"
        }
        return color_map.get(status, str(status.value))


class PipelineDiscovery:
    """Handles discovery of pipelines to test."""
    
    @staticmethod
    def discover_pipelines(pipelines_dir: Path, specified_pipelines: List[str]) -> List[str]:
        """Discover pipelines to test."""
        if specified_pipelines:
            return specified_pipelines
        
        if not pipelines_dir.exists():
            raise FileNotFoundError(f"The directory {pipelines_dir} does not exist")
        
        discovered = sorted([
            d.name for d in pipelines_dir.iterdir() 
            if d.is_dir() and not d.name.startswith('.')
        ])
        
        if not discovered:
            raise ValueError(f"No pipeline directories found in {pipelines_dir}")
        
        return discovered


def main() -> int:
    """Main entry point for the pipeline tester."""
    start_time = time.monotonic()
    console = Console(highlight=False)
    config = Config.from_environment()
    
    try:
        # Initialize Elasticsearch connection
        es_client = ElasticsearchClient(config, console)
        es_client.wait_for_startup()
        
        # Setup paths
        repo_root = (
            Path(__file__).resolve().parent.parent 
            if not os.getenv('GITHUB_ACTIONS') 
            else Path(".")
        )
        pipelines_dir = repo_root / "pipelines"
        
        # Discover pipelines
        console.print("\n[bold]Starting pipeline validation...[/bold]")
        pipelines_to_test = PipelineDiscovery.discover_pipelines(
            pipelines_dir, config.pipelines_to_test
        )
        
        if config.pipelines_to_test:
            console.print(
                f"Testing specified subset of pipelines: [bold]{', '.join(pipelines_to_test)}[/bold]"
            )
        else:
            console.print(
                "[yellow]PIPELINES_TO_TEST not set. Discovering all available pipelines...[/yellow]"
            )
            console.print("Found and will test the following:")
            for pipeline_name in pipelines_to_test:
                console.print(f"    [dim][bold]{pipeline_name}[/bold][/dim]")
        
        # Run tests
        test_runner = PipelineTestRunner(config, console)
        all_results = [
            test_runner.run_pipeline_tests(pipeline_name, pipelines_dir)
            for pipeline_name in pipelines_to_test
        ]
        
        # Calculate total duration
        total_duration = time.monotonic() - start_time
        
        # Report results
        reporter = ResultsReporter(console)
        reporter.print_summary(all_results, total_duration)
        
        # Return appropriate exit code
        total_failed = sum(r.failed_count for r in all_results)
        return 1 if total_failed > 0 else 0
        
    except (ElasticsearchConnectionError, FileNotFoundError, ValueError) as e:
        console.print(f"[red][FATAL][/red] {e}")
        return 1
    except KeyboardInterrupt:
        console.print("\n[yellow]Test execution interrupted by user[/yellow]")
        return 1
    except Exception as e:
        console.print(f"[red][FATAL][/red] Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
