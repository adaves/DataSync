#!/usr/bin/env python3
"""
DataSync Import/Delete Cycle Test Script - Variants B & C

This script provides additional test variants:
- Option B: Alternates between "import all at once" vs "import one at a time"
- Option C: Random choice of import method each iteration

Usage: 
    python test_import_cycle_variants.py --mode B  # Alternating mode
    python test_import_cycle_variants.py --mode C  # Random mode
"""

import sys
import os
import time
import random
import argparse
from datetime import datetime

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import the main test class
from test_import_cycle import ImportCycleTest
import logging

logger = logging.getLogger(__name__)

# Configure logging for variants
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_import_cycle_variants.log'),
        logging.StreamHandler()
    ]
)

class ImportCycleTestVariants(ImportCycleTest):
    """Extended test class with additional import method variants."""
    
    def __init__(self, db_path: str, excel_files: list, iterations: int = 10, mode: str = "B"):
        """Initialize with test mode (B=alternating, C=random)."""
        super().__init__(db_path, excel_files, iterations)
        self.mode = mode
        self.import_methods_used = []
    
    def get_import_method(self, iteration: int) -> str:
        """Get import method based on test mode and iteration."""
        if self.mode == "B":
            # Alternating: odd iterations = all_at_once, even = one_by_one
            method = "all_at_once" if iteration % 2 == 1 else "one_by_one"
        elif self.mode == "C":
            # Random choice
            method = random.choice(["all_at_once", "one_by_one"])
        else:
            method = "all_at_once"  # Default
        
        self.import_methods_used.append(method)
        return method
    
    def run_single_iteration(self, iteration: int) -> bool:
        """Run a single delete/import cycle with variable import method."""
        import_method = self.get_import_method(iteration)
        logger.info(f"\n=== Starting Iteration {iteration}/{self.iterations} (Method: {import_method}) ===")
        
        try:
            db_ops = self.connect_database()
            
            # Step 1: Count initial records
            initial_total = len(db_ops.read_table(self.target_table))
            initial_2025 = self.count_2025_records(db_ops)
            logger.info(f"Initial state: {initial_total:,} total records, {initial_2025:,} from 2025")
            
            # Step 2: Delete 2025 records
            print(f"  Deleting 2025 records...")
            deleted_count, delete_time = self.delete_2025_records(db_ops)
            
            # Step 3: Import records using selected method
            print(f"  Importing Excel files ({import_method})...")
            imported_count, import_time = self.import_excel_files(db_ops, import_method)
            
            # Step 4: Verify import
            print(f"  Verifying import...")
            verification_passed = self.verify_import(db_ops, imported_count)
            
            # Step 5: Final count
            final_total = len(db_ops.read_table(self.target_table))
            final_2025 = self.count_2025_records(db_ops)
            
            # Update statistics
            self.stats['delete_times'].append(delete_time)
            self.stats['import_times'].append(import_time)
            self.stats['records_deleted'].append(deleted_count)
            self.stats['records_imported'].append(imported_count)
            self.stats['total_delete_time'] += delete_time
            self.stats['total_import_time'] += import_time
            
            if not verification_passed:
                self.stats['verification_failures'].append(iteration)
            
            logger.info(f"Iteration {iteration} completed: -{deleted_count:,} +{imported_count:,} records ({import_method})")
            logger.info(f"Final state: {final_total:,} total records, {final_2025:,} from 2025")
            
            db_ops.close()
            return verification_passed
            
        except Exception as e:
            error_msg = f"Iteration {iteration} failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(f"Iteration {iteration}: {str(e)}")
            return False
    
    def _print_summary(self, total_time: float, successful_iterations: int):
        """Print comprehensive test summary with method analysis."""
        print(f"\n{'='*60}")
        print(f"DATASYNC IMPORT/DELETE CYCLE TEST SUMMARY - MODE {self.mode}")
        print(f"{'='*60}")
        
        print(f"\nTEST CONFIGURATION:")
        print(f"  Database: {self.db_path}")
        print(f"  Excel files: {len(self.excel_files)} files")
        print(f"  Target table: {self.target_table}")
        print(f"  Test mode: {self.mode} ({'Alternating' if self.mode == 'B' else 'Random'})")
        print(f"  Planned iterations: {self.iterations}")
        
        print(f"\nOVERALL RESULTS:")
        print(f"  Total test time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        print(f"  Successful iterations: {successful_iterations}/{self.iterations}")
        print(f"  Success rate: {(successful_iterations/self.iterations)*100:.1f}%")
        print(f"  Failed iterations: {self.iterations - successful_iterations}")
        
        # Method usage analysis
        if self.import_methods_used:
            method_counts = {}
            for method in self.import_methods_used:
                method_counts[method] = method_counts.get(method, 0) + 1
            
            print(f"\nIMPORT METHOD USAGE:")
            for method, count in method_counts.items():
                print(f"  {method}: {count} times ({count/len(self.import_methods_used)*100:.1f}%)")
        
        # Performance by method
        if len(self.import_times) == len(self.import_methods_used):
            method_performance = {}
            for i, method in enumerate(self.import_methods_used):
                if method not in method_performance:
                    method_performance[method] = []
                method_performance[method].append(self.stats['import_times'][i])
            
            print(f"\nPERFORMANCE BY METHOD:")
            for method, times in method_performance.items():
                avg_time = sum(times) / len(times)
                print(f"  {method}: {avg_time:.2f}s average ({len(times)} iterations)")
        
        if self.stats['delete_times']:
            print(f"\nDELETE PERFORMANCE:")
            print(f"  Total delete time: {self.stats['total_delete_time']:.2f}s")
            print(f"  Average delete time: {sum(self.stats['delete_times'])/len(self.stats['delete_times']):.2f}s")
            print(f"  Fastest delete: {min(self.stats['delete_times']):.2f}s")
            print(f"  Slowest delete: {max(self.stats['delete_times']):.2f}s")
            print(f"  Total records deleted: {sum(self.stats['records_deleted']):,}")
        
        if self.stats['import_times']:
            print(f"\nIMPORT PERFORMANCE:")
            print(f"  Total import time: {self.stats['total_import_time']:.2f}s")
            print(f"  Average import time: {sum(self.stats['import_times'])/len(self.stats['import_times']):.2f}s")
            print(f"  Fastest import: {min(self.stats['import_times']):.2f}s")
            print(f"  Slowest import: {max(self.stats['import_times']):.2f}s")
            print(f"  Total records imported: {sum(self.stats['records_imported']):,}")
        
        if self.stats['errors']:
            print(f"\nERRORS ENCOUNTERED ({len(self.stats['errors'])}):")
            for error in self.stats['errors']:
                print(f"  - {error}")
        
        if self.stats['verification_failures']:
            print(f"\nVERIFICATION FAILURES:")
            print(f"  Failed iterations: {', '.join(map(str, self.stats['verification_failures']))}")
        
        print(f"\n{'='*60}")
        
        # Save detailed log
        log_file = f"test_results_mode{self.mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(log_file, 'w') as f:
            f.write(f"DataSync Test Results Mode {self.mode} - {datetime.now()}\n")
            f.write(f"Import methods used: {self.import_methods_used}\n")
            f.write(f"Statistics: {self.stats}\n")
        print(f"Detailed results saved to: {log_file}")

def main():
    """Main function to run the variant tests."""
    parser = argparse.ArgumentParser(description="DataSync Import/Delete Cycle Test - Variants")
    parser.add_argument("--mode", choices=["B", "C"], required=True,
                       help="Test mode: B=alternating, C=random")
    parser.add_argument("--iterations", type=int, default=10,
                       help="Number of test iterations (default: 10)")
    args = parser.parse_args()
    
    # Configuration
    DATABASE_PATH = "working db 8.25.2025 prodjectDataPTP.accdb"
    EXCEL_FILES = [
        "8.18.25 - all other - Projected KPI's COT - TG Fixed Public - Default.xls",
        "8.18.25 - groc all other - Projected KPI's COT - TG Fixed Public - Default.xls", 
        "8.18.25 - grocery enlow - Projected KPI's COT - TG Fixed Public - Default.xls"
    ]
    
    # Verify files exist
    if not os.path.exists(DATABASE_PATH):
        print(f"❌ Database file not found: {DATABASE_PATH}")
        return 1
    
    missing_files = [f for f in EXCEL_FILES if not os.path.exists(f)]
    if missing_files:
        print(f"❌ Excel files not found: {', '.join(missing_files)}")
        return 1
    
    # Set random seed for reproducible results in mode C
    if args.mode == "C":
        random.seed(42)
        print(f"🎲 Random seed set to 42 for reproducible results")
    
    # Run the test
    mode_name = "Alternating" if args.mode == "B" else "Random"
    print(f"🚀 Starting DataSync Import/Delete Cycle Test - Mode {args.mode} ({mode_name})")
    print(f"   Database: {DATABASE_PATH}")
    print(f"   Excel files: {len(EXCEL_FILES)} files")
    print(f"   Iterations: {args.iterations}")
    
    test = ImportCycleTestVariants(DATABASE_PATH, EXCEL_FILES, args.iterations, args.mode)
    test.run_test()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
