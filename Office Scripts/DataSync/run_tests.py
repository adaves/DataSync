#!/usr/bin/env python3
"""
DataSync Test Runner

Quick runner for all test variants.

Usage:
    python run_tests.py          # Run Option A (all at once, 10 iterations)
    python run_tests.py -a       # Run Option A only
    python run_tests.py -b       # Run Option B only (alternating)
    python run_tests.py -c       # Run Option C only (random)
    python run_tests.py -all     # Run all three options
    python run_tests.py -n 5     # Run with 5 iterations instead of 10
"""

import argparse
import subprocess
import sys
import os

def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=False)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False

def main():
    parser = argparse.ArgumentParser(description="DataSync Test Runner")
    parser.add_argument("-a", "--option-a", action="store_true", 
                       help="Run Option A: All files at once")
    parser.add_argument("-b", "--option-b", action="store_true",
                       help="Run Option B: Alternating import methods")
    parser.add_argument("-c", "--option-c", action="store_true",
                       help="Run Option C: Random import methods")
    parser.add_argument("-all", "--all-options", action="store_true",
                       help="Run all three options")
    parser.add_argument("-n", "--iterations", type=int, default=10,
                       help="Number of iterations per test (default: 10)")
    
    args = parser.parse_args()
    
    # If no specific option is chosen, default to Option A
    if not any([args.option_a, args.option_b, args.option_c, args.all_options]):
        args.option_a = True
    
    # Check if required files exist
    required_files = [
        "working db 8.25.2025 prodjectDataPTP.accdb",
        "8.18.25 - all other - Projected KPI's COT - TG Fixed Public - Default.xls",
        "8.18.25 - groc all other - Projected KPI's COT - TG Fixed Public - Default.xls",
        "8.18.25 - grocery enlow - Projected KPI's COT - TG Fixed Public - Default.xls"
    ]
    
    missing_files = [f for f in required_files if not os.path.exists(f)]
    if missing_files:
        print(f"❌ Required files not found:")
        for f in missing_files:
            print(f"   - {f}")
        return 1
    
    print(f"🔍 All required files found")
    print(f"📊 Test configuration: {args.iterations} iterations per test")
    
    success_count = 0
    total_tests = 0
    
    # Run Option A
    if args.option_a or args.all_options:
        total_tests += 1
        if run_command("python test_import_cycle.py", "Option A: Import all files at once"):
            success_count += 1
    
    # Run Option B
    if args.option_b or args.all_options:
        total_tests += 1
        cmd = f"python test_import_cycle_variants.py --mode B --iterations {args.iterations}"
        if run_command(cmd, "Option B: Alternating import methods"):
            success_count += 1
    
    # Run Option C
    if args.option_c or args.all_options:
        total_tests += 1
        cmd = f"python test_import_cycle_variants.py --mode C --iterations {args.iterations}"
        if run_command(cmd, "Option C: Random import methods"):
            success_count += 1
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"📋 FINAL TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests completed: {success_count}/{total_tests}")
    print(f"Success rate: {(success_count/total_tests)*100:.1f}%")
    
    if success_count == total_tests:
        print(f"🎉 All tests completed successfully!")
        return 0
    else:
        print(f"⚠️  {total_tests - success_count} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
