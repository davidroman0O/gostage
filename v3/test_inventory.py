#!/usr/bin/env python3
"""
Test Inventory and Flakiness Detection Script

Discovers all test functions in v3/, runs each individually 20 times,
and generates inventory and failure reports.
"""

import subprocess
import re
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime
import time

# Base directory
BASE_DIR = Path(__file__).parent.parent
V3_DIR = BASE_DIR / "v3"
DOCS_DIR = V3_DIR / "docs"

def get_packages():
    """Get all packages in v3/ that have test files."""
    packages = set()
    for test_file in V3_DIR.rglob("*_test.go"):
        if ".gocache" in str(test_file) or "examples" in str(test_file):
            continue
        # Get package path (directory containing test file)
        pkg_path = test_file.parent.relative_to(BASE_DIR)
        packages.add(str(pkg_path))
    return sorted(packages)

def discover_tests(package_path):
    """Discover all test functions in a package by parsing test files."""
    tests = []
    pkg_dir = BASE_DIR / package_path
    
    if not pkg_dir.exists():
        return tests
    
    # Find all test files in the package
    test_files = list(pkg_dir.glob("*_test.go"))
    
    for test_file in test_files:
        try:
            content = test_file.read_text()
            # Find all test functions: func TestXxx(t *testing.T) or func BenchmarkXxx(b *testing.B)
            # Also handle subtests: t.Run("subtest", func(t *testing.T))
            test_pattern = r'func\s+(Test\w+)\s*\([^)]*\)'
            benchmark_pattern = r'func\s+(Benchmark\w+)\s*\([^)]*\)'
            
            for match in re.finditer(test_pattern, content):
                test_name = match.group(1)
                if test_name not in tests:
                    tests.append(test_name)
            
            for match in re.finditer(benchmark_pattern, content):
                bench_name = match.group(1)
                if bench_name not in tests:
                    tests.append(bench_name)
        except Exception as e:
            print(f"  Warning: Could not parse {test_file}: {e}")
    
    return sorted(tests)

def run_test(package_path, test_name, count=20):
    """Run a test function count times and return results."""
    results = {
        "package": package_path,
        "test": test_name,
        "total_runs": count,
        "passed": 0,
        "failed": 0,
        "errors": [],
        "execution_times": []
    }
    
    for i in range(count):
        start_time = time.time()
        try:
            result = subprocess.run(
                ["go", "test", "-run", f"^{test_name}$", "-count", "1", f"./{package_path}"],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout per test run
                env={**os.environ, "GOFLAGS": "-count=1"}  # Ensure no caching
            )
            elapsed = time.time() - start_time
            results["execution_times"].append(elapsed)
            
            if result.returncode == 0:
                results["passed"] += 1
            else:
                results["failed"] += 1
                # Extract error message
                error_msg = result.stderr if result.stderr else result.stdout
                if error_msg:
                    # Get first few lines of error
                    error_lines = error_msg.splitlines()[:10]
                    error_summary = "\n".join(error_lines)
                    if error_summary not in results["errors"]:
                        results["errors"].append(error_summary)
        except subprocess.TimeoutExpired:
            elapsed = time.time() - start_time
            results["execution_times"].append(elapsed)
            results["failed"] += 1
            error_msg = "Test timeout (5 minutes)"
            if error_msg not in results["errors"]:
                results["errors"].append(error_msg)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            elapsed = time.time() - start_time if 'start_time' in locals() else 0
            results["execution_times"].append(elapsed)
            results["failed"] += 1
            error_msg = f"Execution error: {str(e)}"
            if error_msg not in results["errors"]:
                results["errors"].append(error_msg)
    
    return results

def build_inventory():
    """Build complete test inventory."""
    print("Discovering all test files and functions...")
    inventory = defaultdict(lambda: defaultdict(list))
    
    packages = get_packages()
    total_tests = 0
    
    for package_path in packages:
        print(f"  Discovering tests in {package_path}...")
        tests = discover_tests(package_path)
        
        # Get test files in this package
        pkg_dir = BASE_DIR / package_path
        test_files = []
        if pkg_dir.exists():
            for test_file in pkg_dir.glob("*_test.go"):
                test_files.append(test_file.name)
        
        if test_files:
            # Group tests by file (we'll list all tests under the package)
            inventory[package_path]["test_files"] = sorted(test_files)
            inventory[package_path]["tests"] = sorted(tests)
            total_tests += len(tests)
    
    return inventory, total_tests

def generate_inventory_md(inventory, total_tests):
    """Generate test inventory markdown."""
    lines = [
        "# Test Inventory",
        "",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Summary",
        f"- Total Packages: {len(inventory)}",
        f"- Total Test Functions: {total_tests}",
        "",
        "## Packages",
        ""
    ]
    
    for package_path in sorted(inventory.keys()):
        pkg_data = inventory[package_path]
        test_files = pkg_data.get("test_files", [])
        tests = pkg_data.get("tests", [])
        
        lines.append(f"### {package_path}")
        lines.append(f"- Test Files: {len(test_files)}")
        lines.append(f"- Test Functions: {len(tests)}")
        lines.append("")
        
        for test_file in test_files:
            lines.append(f"#### {test_file}")
            # Find tests that might be in this file (we can't be 100% sure without parsing)
            # List all tests for now
            for test in tests:
                lines.append(f"  - {test}")
            lines.append("")
    
    return "\n".join(lines)

def generate_failures_md(failure_results):
    """Generate failing tests markdown."""
    if not failure_results:
        return "# Failing Tests Report\n\nNo failing tests found.\n"
    
    total_tests_run = sum(r["total_runs"] for r in failure_results)
    flaky_tests = [r for r in failure_results if 0 < r["failed"] < r["total_runs"]]
    consistently_failing = [r for r in failure_results if r["failed"] == r["total_runs"]]
    
    lines = [
        "# Failing Tests Report",
        "",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Summary",
        f"- Total Tests Run: {total_tests_run}",
        f"- Flaky Tests: {len(flaky_tests)}",
        f"- Consistently Failing: {len(consistently_failing)}",
        "",
    ]
    
    if flaky_tests:
        lines.extend([
            "## Flaky Tests",
            ""
        ])
        
        # Group by package
        by_package = defaultdict(list)
        for result in flaky_tests:
            by_package[result["package"]].append(result)
        
        for package_path in sorted(by_package.keys()):
            lines.append(f"### {package_path}")
            lines.append("")
            
            for result in sorted(by_package[package_path], key=lambda x: x["test"]):
                failure_rate = result["failed"] / result["total_runs"] * 100
                avg_time = sum(result["execution_times"]) / len(result["execution_times"]) if result["execution_times"] else 0
                
                lines.append(f"#### {result['test']}")
                lines.append(f"- Failure Rate: {result['failed']}/{result['total_runs']} ({failure_rate:.1f}%)")
                lines.append(f"- Average Execution Time: {avg_time:.3f}s")
                lines.append("- Errors:")
                for error in result["errors"][:3]:  # Limit to 3 error samples
                    lines.append(f"  ```")
                    lines.append(f"  {error[:500]}")  # Limit error length
                    lines.append(f"  ```")
                lines.append("")
    
    if consistently_failing:
        lines.extend([
            "## Consistently Failing Tests",
            ""
        ])
        
        by_package = defaultdict(list)
        for result in consistently_failing:
            by_package[result["package"]].append(result)
        
        for package_path in sorted(by_package.keys()):
            lines.append(f"### {package_path}")
            lines.append("")
            
            for result in sorted(by_package[package_path], key=lambda x: x["test"]):
                lines.append(f"#### {result['test']}")
                lines.append(f"- Failure Rate: {result['failed']}/{result['total_runs']} (100%)")
                lines.append("- Errors:")
                for error in result["errors"][:2]:  # Limit to 2 error samples
                    lines.append(f"  ```")
                    lines.append(f"  {error[:500]}")
                    lines.append(f"  ```")
                lines.append("")
    
    return "\n".join(lines)

def main():
    print("=" * 80)
    print("Test Inventory and Flakiness Detection")
    print("=" * 80)
    
    # Step 1: Build inventory
    print("\n[1/4] Building test inventory...")
    inventory, total_tests = build_inventory()
    print(f"Found {total_tests} test functions across {len(inventory)} packages")
    
    # Step 2: Generate inventory document
    print("\n[2/4] Generating test inventory document...")
    inventory_md = generate_inventory_md(inventory, total_tests)
    DOCS_DIR.mkdir(exist_ok=True)
    inventory_path = DOCS_DIR / "test-inventory.md"
    inventory_path.write_text(inventory_md)
    print(f"Saved to {inventory_path}")
    
    # Step 3: Run all tests
    print("\n[3/4] Running all tests individually (20 times each)...")
    print("This may take a while...")
    print("Press Ctrl+C to stop and save progress\n")
    failure_results = []
    test_count = 0
    progress_file = DOCS_DIR / "test-progress.json"
    
    # Load progress if exists
    completed_tests = set()
    if progress_file.exists():
        try:
            with open(progress_file, 'r') as f:
                progress_data = json.load(f)
                completed_tests = set(progress_data.get("completed", []))
                failure_results = progress_data.get("failures", [])
                test_count = progress_data.get("test_count", 0)
                print(f"Resuming from test {test_count + 1}...")
        except Exception as e:
            print(f"Warning: Could not load progress: {e}")
    
    try:
        for package_path in sorted(inventory.keys()):
            tests = inventory[package_path].get("tests", [])
            for test_name in tests:
                test_key = f"{package_path}::{test_name}"
                
                # Skip if already completed
                if test_key in completed_tests:
                    continue
                
                test_count += 1
                print(f"[{test_count}/{total_tests}] Running {test_key}...", end=" ", flush=True)
                sys.stdout.flush()
                
                try:
                    result = run_test(package_path, test_name, count=20)
                    completed_tests.add(test_key)
                    
                    if result["failed"] > 0:
                        failure_results.append(result)
                        print(f"FAILED: {result['failed']}/20 runs failed")
                    else:
                        print(f"PASSED: 20/20 runs passed")
                    
                    # Save progress after each test
                    with open(progress_file, 'w') as f:
                        json.dump({
                            "completed": list(completed_tests),
                            "failures": failure_results,
                            "test_count": test_count,
                            "total_tests": total_tests
                        }, f, indent=2)
                    
                except KeyboardInterrupt:
                    print("\n\nInterrupted by user. Saving progress...")
                    break
                except Exception as e:
                    print(f"ERROR: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    # Create a failure result for this error
                    failure_results.append({
                        "package": package_path,
                        "test": test_name,
                        "total_runs": 0,
                        "passed": 0,
                        "failed": 0,
                        "errors": [f"Script error: {str(e)}"],
                        "execution_times": []
                    })
                    completed_tests.add(test_key)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Saving progress...")
    finally:
        # Final save
        try:
            with open(progress_file, 'w') as f:
                json.dump({
                    "completed": list(completed_tests),
                    "failures": failure_results,
                    "test_count": test_count,
                    "total_tests": total_tests
                }, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save progress: {e}")
    
    # Step 4: Generate failures document
    print("\n[4/4] Generating failing tests document...")
    failures_md = generate_failures_md(failure_results)
    failures_path = DOCS_DIR / "failing-tests.md"
    failures_path.write_text(failures_md)
    print(f"Saved to {failures_path}")
    
    print("\n" + "=" * 80)
    print("Complete!")
    print(f"- Test Inventory: {inventory_path}")
    print(f"- Failing Tests: {failures_path}")
    print(f"- Total Tests Run: {test_count * 20}")
    print(f"- Flaky/Failing Tests: {len(failure_results)}")
    print("=" * 80)

if __name__ == "__main__":
    main()

