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
class TestSwitches:
    """Easy on/off switches for categories of dynamic tests."""
    idempotency: bool = True
    on_failure: bool = True
    performance: bool = True


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
    tests: TestSwitches = field(default_factory=TestSwitches)

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
        self.console.print("Waiting for Elasticsearch to start...")

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
        self.console.print("Waiting for Elasticsearch cluster to be healthy...")

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
        # The result of a simulation is a list of docs
        if isinstance(result, dict) and 'docs' in result:
             return [cls.normalize(doc) for doc in result['docs']]

        # A single document from the simulation
        if isinstance(result, dict) and 'doc' in result:
            return cls.normalize(result['doc'])

        # The _source of a document
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

        # Pass the raw diff object for detailed formatting
        return ResultComparator._format_diff_message(diff, test_name)

    @staticmethod
    def _format_diff_message(diff: DeepDiff, test_name: str) -> str:
        """Format DeepDiff output into a more readable error message."""
        error_parts = [f"[bold]Test '{test_name}' failed with the following differences:[/bold]"]

        # Handle field mismatches (added + removed = likely renamed/typo)
        fields_only_in_actual = {ResultComparator._clean_path(str(item)) for item in diff.get('dictionary_item_added', [])}
        fields_only_in_expected = {ResultComparator._clean_path(str(item)) for item in diff.get('dictionary_item_removed', [])}

        if fields_only_in_actual and fields_only_in_expected:
            error_parts.append("\n[cyan]🔄 Field mismatches (likely renamed):[/cyan]")
            actual_list = sorted(list(fields_only_in_actual))
            expected_list = sorted(list(fields_only_in_expected))
            if len(actual_list) == len(expected_list):
                 for found, expected in zip(expected_list, actual_list):
                    error_parts.append(f"   [yellow]Found[/yellow]: '{found}' but [yellow]Expected[/yellow]: '{expected}'")
                 fields_only_in_actual.clear()
                 fields_only_in_expected.clear()

        if fields_only_in_actual:
            error_parts.append("\n[green]📝 Unexpected fields found in result:[/green]")
            for item in sorted(list(fields_only_in_actual)): error_parts.append(f"   [green]+ {item}[/green]")

        if fields_only_in_expected:
            error_parts.append("\n[red]❌ Expected fields missing from result:[/red]")
            for item in sorted(list(fields_only_in_expected)): error_parts.append(f"   [red]- {item}[/red]")

        if 'values_changed' in diff:
            error_parts.append("\n[magenta]🔄 Field values that differ:[/magenta]")
            for path, change in diff['values_changed'].items():
                clean_path = ResultComparator._clean_path(str(path))
                old_val_str = json.dumps(change.get('old_value'))
                new_val_str = json.dumps(change.get('new_value'))
                error_parts.append(f"   '{clean_path}':")
                error_parts.append(f"     [red]- Expected: {old_val_str}[/red]")
                error_parts.append(f"     [green]+ Found:    {new_val_str}[/green]")


        if 'type_changes' in diff:
            error_parts.append("\n[blue]🔀 Field type mismatches:[/blue]")
            for path, change in diff['type_changes'].items():
                clean_path = ResultComparator._clean_path(str(path))
                old_type = change.get('old_type', type(None)).__name__
                new_type = change.get('new_type', type(None)).__name__
                error_parts.append(f"   '{clean_path}': Expected [cyan]{old_type}[/cyan], but found [cyan]{new_type}[/cyan]")

        return "\n".join(error_parts)

    @staticmethod
    def _clean_path(path: str) -> str:
        """Clean up DeepDiff path format to be more readable."""
        cleaned = path.replace("root", "").lstrip("['").rstrip("']")
        cleaned = re.sub(r"'\]\['", r".", cleaned)
        return cleaned if cleaned else "_source"


class TestGenerator:
    """Generates dynamic test cases for pipeline testing."""

    @staticmethod
    def generate_base_dynamic_tests(happy_path_doc: Dict[str, Any]) -> List[tuple[Dict[str, Any], str]]:
        """Generate common dynamic test cases."""
        source_doc = happy_path_doc.get("docs", [{}])[0].get("_source", {})
        primary_fields = list(source_doc.keys())
        test_cases = [
            ({"docs": [{"_source": {}}]}, "Empty Document"),
            ({"docs": [{"_source": {"foo": "bar"}}]}, "Unrelated Fields"),
        ]
        doc_with_extra = deepcopy(happy_path_doc)
        doc_with_extra["docs"][0]["_source"]["zz_test_field"] = "preserved"
        test_cases.append((doc_with_extra, "Extra Field Preservation"))
        test_cases.extend([
            ({"docs": [{"_source": {field: None}}]}, f"Null Value for '{field}'") for field in primary_fields
        ])
        return test_cases

    @staticmethod
    def generate_on_failure_tests(pipeline_data: Dict[str, Any]) -> List[tuple[Dict[str, Any], str, str]]:
        """Generate tests to check 'on_failure' blocks."""
        failure_tests = []
        for i, processor_wrapper in enumerate(pipeline_data.get("processors", [])):
            processor_name, processor_config = list(processor_wrapper.items())[0]
            if "on_failure" not in processor_config:
                continue

            failure_handler = processor_config["on_failure"][0]
            set_processor = failure_handler.get("set", {})
            target_field = set_processor.get("field")
            expected_value = set_processor.get("value")

            if not target_field or not expected_value:
                continue

            source_field = processor_config.get("field")
            if not source_field: continue # Cannot create test if source field is unknown

            # Create "poison pill" inputs for common processors
            poison_pill, test_name = None, None
            if processor_name == "grok":
                poison_pill = {"docs": [{"_source": {source_field: "not a matching pattern"}}]}
                test_name = f"on_failure/grok-processor-{i+1}"
            elif processor_name == "json":
                poison_pill = {"docs": [{"_source": {source_field: "not valid json"}}]}
                test_name = f"on_failure/json-processor-{i+1}"
            elif processor_name == "date":
                poison_pill = {"docs": [{"_source": {source_field: "not a valid date"}}]}
                test_name = f"on_failure/date-processor-{i+1}"
            elif processor_name == "convert":
                target_type = processor_config.get("type")
                value = "not a number" if target_type in ["integer", "float"] else 12345
                poison_pill = {"docs": [{"_source": {source_field: value}}]}
                test_name = f"on_failure/convert-processor-{i+1}"

            if poison_pill and test_name:
                failure_tests.append((poison_pill, test_name, f"docs[0].doc._source.{target_field}"))

        return failure_tests


class PipelineTestRunner:
    """Main test runner for pipeline validation."""

    def __init__(self, config: Config, console: Console):
        self.config = config
        self.console = console
        self.es_client = ElasticsearchClient(config, console)
        self.pipeline_loader = PipelineLoader()
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

        if not pipeline_file.exists():
            self._add_skip_result(results, "Missing pipeline.json")
            return results

        try:
            pipeline_data = self.pipeline_loader.load_pipeline(pipeline_file)
        except PipelineLoadError as e:
            self._add_error_result(results, "Pipeline Load Failure", str(e), pipeline_file)
            return results

        self._run_static_tests(results, pipeline_path, pipeline_data)

        happy_path_files = sorted(pipeline_path.glob("simulate_example_happy_path_*.json"))
        if happy_path_files and results.overall_status != TestStatus.FAILED:
            happy_path_doc = self.pipeline_loader.load_json_file(happy_path_files[0])
            self._run_dynamic_tests_orchestrator(results, pipeline_data, happy_path_doc)
        elif not happy_path_files:
            self.console.print(
                "[yellow][SKIP][/yellow] No 'happy_path' file found. Skipping dynamic & performance tests."
            )
        return results

    def _add_skip_result(self, results: PipelineTestResults, reason: str) -> None:
        self.console.print(f"[yellow][SKIP][/yellow] Directory '{results.pipeline_name}' skipped ({reason})")
        results.test_results.append(TestResult(name=f"Setup/{reason}", status=TestStatus.FAILED))

    def _add_error_result(self, results: PipelineTestResults, name: str, msg: str, file: Path) -> None:
        log_github_error(msg, file)
        self.console.print(Panel(f"[bold]Fatal error.[/bold]\n\nDetails: {msg}", title="[red]Setup Failure", border_style="red"))
        results.test_results.append(TestResult(name=f"Setup/{name}", status=TestStatus.FAILED, error_message=msg))

    def _run_static_tests(self, results: PipelineTestResults, path: Path, pipeline: Dict) -> None:
        static_files = sorted(path.glob("simulate_example_*.json"))
        if not static_files: return

        with self.console.status("[yellow]Running Static Tests...", spinner="dots"):
             for example_file in static_files:
                test_name = example_file.stem.replace("simulate_example_", "")
                try:
                    results_file = example_file.with_name(f"simulate_results_{test_name}.json")
                    if not results_file.exists():
                        raise FileNotFoundError(f"Missing results file: {results_file.name}")
                    example_data = self.pipeline_loader.load_json_file(example_file)
                    expected = self.pipeline_loader.load_json_file(results_file)
                    actual = self.es_client.simulate_pipeline(pipeline, example_data)
                    error = ResultComparator.compare_results(expected, actual, f"static/{test_name}")
                    if error: raise PipelineTestError(error)
                    results.test_results.append(TestResult(name=f"static/{test_name}", status=TestStatus.PASSED))
                except (PipelineLoadError, FileNotFoundError, PipelineTestError) as e:
                    self._handle_test_failure(results, f"static/{test_name}", str(e))
                    break # Stop on first static failure

        passed = sum(1 for r in results.test_results if r.name.startswith("static/") and r.status == TestStatus.PASSED)
        status_icon = "[red][✗][/red]" if results.failed_count > 0 else f"[green][✓][/green]"
        self.console.print(f"{status_icon} Static Tests ({passed}/{len(static_files)} passed)")

    def _run_dynamic_tests_orchestrator(self, results, pipeline_data, happy_path_doc):
        # Base Dynamic Tests
        self._run_base_dynamic_tests(results, pipeline_data, happy_path_doc)
        if results.overall_status == TestStatus.FAILED: return

        # Idempotency Test
        if self.config.tests.idempotency:
            self._run_idempotency_test(results, pipeline_data, happy_path_doc)
            if results.overall_status == TestStatus.FAILED: return

        # On-Failure Tests
        if self.config.tests.on_failure:
            self._run_on_failure_tests(results, pipeline_data)
            if results.overall_status == TestStatus.FAILED: return

        # Performance Test
        if self.config.tests.performance:
            self._run_performance_test(results, pipeline_data, happy_path_doc)


    def _run_base_dynamic_tests(self, results, pipeline_data, happy_path_doc):
        test_cases = self.test_generator.generate_base_dynamic_tests(happy_path_doc)
        with Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.percentage:>3.0f}%")) as p:
            task_id = p.add_task("[yellow]Running Dynamic Tests...", total=len(test_cases))
            for input_doc, test_name in test_cases:
                p.update(task_id, advance=1)
                try:
                    result1 = self.es_client.simulate_pipeline(pipeline_data, input_doc)
                    result2 = self.es_client.simulate_pipeline(pipeline_data, input_doc)
                    error = ResultComparator.compare_results(result1, result2, test_name)
                    if error: raise PipelineTestError(f"Consistency check failed: {error}")
                    results.test_results.append(TestResult(name=f"dynamic/{test_name}", status=TestStatus.PASSED))
                except (PipelineTestError, ElasticsearchConnectionError) as e:
                    p.update(task_id, description="[red][✗][/red] Dynamic Tests")
                    self._handle_test_failure(results, f"dynamic/{test_name}", str(e))
                    return
            p.update(task_id, description=f"[green][✓][/green] Dynamic Tests ({len(test_cases)} passed)")

    def _run_idempotency_test(self, results, pipeline_data, happy_path_doc):
        test_name = "dynamic/Idempotency"
        with self.console.status("[yellow]Running Idempotency Test...", spinner="dots"):
            try:
                result_a = self.es_client.simulate_pipeline(pipeline_data, happy_path_doc)
                source_from_a = {"docs": [result_a['docs'][0]['doc']]}
                result_b = self.es_client.simulate_pipeline(pipeline_data, source_from_a)
                error = ResultComparator.compare_results(result_a, result_b, test_name)
                if error:
                    msg = "Pipeline is not idempotent. Running a document through a second time changed the result."
                    raise PipelineTestError(f"{msg}\n\n{error}")
                results.test_results.append(TestResult(name=test_name, status=TestStatus.PASSED))
                self.console.print("[green][✓][/green] Idempotency Test")
            except (PipelineTestError, ElasticsearchConnectionError) as e:
                self.console.print("[red][✗][/red] Idempotency Test")
                self._handle_test_failure(results, test_name, str(e))

    def _run_on_failure_tests(self, results, pipeline_data):
        failure_tests = self.test_generator.generate_on_failure_tests(pipeline_data)
        if not failure_tests: return
        with self.console.status("[yellow]Running On-Failure Tests...", spinner="dots"):
            for pill, test_name, target_field in failure_tests:
                try:
                    result = self.es_client.simulate_pipeline(pipeline_data, pill)
                    # Check if the expected failure tag was set by traversing the path
                    tag_value = result
                    for key in target_field.split('.'):
                        if isinstance(tag_value, list) and key.isdigit():
                            tag_value = tag_value[int(key)]
                        else:
                            tag_value = tag_value.get(key)

                    if tag_value is None:
                        raise PipelineTestError(f"Expected failure tag '{target_field}' was not set.")
                    results.test_results.append(TestResult(name=test_name, status=TestStatus.PASSED))
                except (PipelineTestError, ElasticsearchConnectionError, KeyError, IndexError) as e:
                    self.console.print(f"[red][✗][/red] On-Failure Tests")
                    self._handle_test_failure(results, test_name, str(e))
                    return # Stop on first failure
            self.console.print("[green][✓][/green] On-Failure Tests")

    def _run_performance_test(self, results, pipeline_data, happy_path_doc):
        try:
            with self.console.status("[yellow]Running Performance Test...", spinner="dots"):
                start_time = time.monotonic()
                for _ in range(self.config.performance_test_iterations):
                    self.es_client.simulate_pipeline(pipeline_data, happy_path_doc)
                duration = time.monotonic() - start_time
            docs_per_sec = self.config.performance_test_iterations / duration
            avg_time_ms = (duration / self.config.performance_test_iterations) * 1000
            self.console.print("[green][✓][/green] Performance Test")
            self.console.print(f"      [dim]↳ Avg. Time/Document: {avg_time_ms:.2f} ms | Throughput: {docs_per_sec:.2f} docs/sec[/dim]")
            results.test_results.append(TestResult(name="performance", status=TestStatus.PASSED))
        except (PipelineTestError, ElasticsearchConnectionError) as e:
            self.console.print("[red][✗][/red] Performance Test")
            self._handle_test_failure(results, "performance", str(e))

    def _handle_test_failure(self, results: PipelineTestResults, test_name: str, error_msg: str) -> None:
        log_github_error(error_msg, Path("pipeline.json"))
        results.test_results.append(TestResult(name=test_name, status=TestStatus.FAILED, error_message=error_msg))
        self.console.print(Panel(error_msg, title=f"[red]Test Failure: {test_name}", border_style="red", expand=False))


class ResultsReporter:
    """Handles reporting of test results."""
    def __init__(self, console: Console):
        self.console = console

    def print_summary(self, all_results: List[PipelineTestResults], total_duration: float) -> None:
        total_passed = sum(r.passed_count for r in all_results)
        total_failed = sum(r.failed_count for r in all_results)
        self.console.print("\n--- [bold]Final Test Summary[/bold] ---")
        table = self._create_summary_table(all_results, total_passed, total_failed, total_duration)
        self.console.print(table)

        if total_failed > 0:
            self.console.print("\n[bold red]Failed Test Details:[/bold red]")
            for r in all_results:
                if r.failed_count > 0:
                    for failed_test in r.test_results:
                        if failed_test.status == TestStatus.FAILED:
                            self.console.print(f" - {r.pipeline_name} / {failed_test.name}")
        else:
            self.console.print("\n[bold green]ALL TESTS PASSED[/bold green]")

    def _create_summary_table(self, all_results, total_passed, total_failed, total_duration) -> Table:
        table = Table(title="Per-Pipeline Results", show_header=True, header_style="bold cyan", show_footer=True, footer_style="bold")
        table.add_column("Pipeline", footer="Overall Totals", no_wrap=True)
        table.add_column("Status", justify="center")
        table.add_column("Passed", justify="right", footer=f"[green]{total_passed}[/green]")
        table.add_column("Failed", justify="right", footer=f"[red]{total_failed}[/red]" if total_failed else "0")
        table.add_column("Duration (s)", justify="right", footer=f"{total_duration:.2f}s")

        for result in all_results:
            color_map = {TestStatus.PASSED: "[green]PASSED", TestStatus.FAILED: "[red]FAILED", TestStatus.SKIPPED: "[yellow]SKIPPED"}
            status_display = color_map.get(result.overall_status, str(result.overall_status.value))
            table.add_row(result.pipeline_name, status_display, str(result.passed_count), str(result.failed_count), f"{result.duration:.2f}s")
        return table


class PipelineDiscovery:
    """Handles discovery of pipelines to test."""
    @staticmethod
    def discover_pipelines(pipelines_dir: Path, specified_pipelines: List[str]) -> List[str]:
        if specified_pipelines:
            return specified_pipelines
        if not pipelines_dir.exists():
            raise FileNotFoundError(f"The directory {pipelines_dir} does not exist")
        discovered = sorted([d.name for d in pipelines_dir.iterdir() if d.is_dir() and not d.name.startswith('.')])
        if not discovered:
            raise ValueError(f"No pipeline directories found in {pipelines_dir}")
        return discovered


def main() -> int:
    """Main entry point for the pipeline tester."""
    overall_start_time = time.monotonic()
    console = Console(highlight=False)
    config = Config.from_environment()

    try:
        es_client = ElasticsearchClient(config, console)
        es_client.wait_for_startup()
        repo_root = Path(__file__).resolve().parent.parent if not os.getenv('GITHUB_ACTIONS') else Path(".")
        pipelines_dir = repo_root / "pipelines"

        console.print("\n[bold]Starting pipeline validation...[/bold]")
        pipelines_to_test = PipelineDiscovery.discover_pipelines(pipelines_dir, config.pipelines_to_test)

        if config.pipelines_to_test:
            console.print(f"Testing specified subset of pipelines: [bold]{', '.join(pipelines_to_test)}[/bold]")
        else:
            console.print("[yellow]PIPELINES_TO_TEST not set. Discovering all available pipelines...[/yellow]")
            console.print(f"Found and will test the following: [bold]{', '.join(pipelines_to_test)}[/bold]")

        test_runner = PipelineTestRunner(config, console)
        all_results = [
            test_runner.run_pipeline_tests(pipeline_name, pipelines_dir) for pipeline_name in pipelines_to_test
        ]

        total_duration = time.monotonic() - overall_start_time
        reporter = ResultsReporter(console)
        reporter.print_summary(all_results, total_duration)

        return 1 if any(r.failed_count > 0 for r in all_results) else 0

    except (ElasticsearchConnectionError, FileNotFoundError, ValueError) as e:
        console.print(f"[red][FATAL][/red] {e}")
        return 1
    except KeyboardInterrupt:
        console.print("\n[yellow]Test execution interrupted by user[/yellow]")
        return 1
    except Exception as e:
        console.print(f"[red][FATAL UNHANDLED][/red] {e}")
        # For debugging, print full traceback for unexpected errors
        console.print_exception()
        return 1


if __name__ == "__main__":
    sys.exit(main())
