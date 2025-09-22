#!/usr/bin/env python3
"""
DataSync Import/Delete Cycle Test Script

This script tests the reliability and performance of the DataSync import/delete functionality
by repeatedly deleting all 2025 records and re-importing them from Excel files.

Test Modes:
- Mode A (default): Import all files at once every iteration
- Mode B: Alternating between "all at once" and "one by one" methods
- Mode C: Random choice of import method each iteration

Usage: 
    # Interactive mode (prompts for settings)
    python test_import_cycle.py                    # Auto-interactive if no args
    python test_import_cycle.py --interactive      # Force interactive mode
    
    # Command line mode (direct execution)
    python test_import_cycle.py --mode A           # Mode A (all at once)
    python test_import_cycle.py --mode B           # Mode B (alternating)
    python test_import_cycle.py --mode C           # Mode C (random)
    python test_import_cycle.py --iterations 20    # Custom iteration count
    python test_import_cycle.py --mode B --iterations 50  # Combined options
"""

import sys
import os
import time
import logging
import random
import argparse
from datetime import datetime
from typing import Dict, List, Tuple

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.database.operations import DatabaseOperations
from datasync.processing.excel_processor import ExcelProcessor

# Configure logging (file only to reduce console spam)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_import_cycle.log')
    ]
)
logger = logging.getLogger(__name__)

class ImportCycleTest:
    def __init__(self, db_path: str, excel_files: List[str], iterations: int = 10, mode: str = "A"):
        """Initialize the test with database path, Excel files, and test mode."""
        self.db_path = db_path
        self.excel_files = excel_files
        self.iterations = iterations
        self.mode = mode.upper()
        self.target_table = "tblProjectedDataPTP"
        
        # Track import methods used for analysis
        self.import_methods_used = []
        
        # Test statistics
        self.stats = {
            'iterations_completed': 0,
            'total_delete_time': 0.0,
            'total_import_time': 0.0,
            'delete_times': [],
            'import_times': [],
            'records_deleted': [],
            'records_imported': [],
            'errors': [],
            'verification_failures': []
        }
        
    def get_import_method(self, iteration: int) -> str:
        """Get import method based on test mode and iteration."""
        if self.mode == "A":
            # Mode A: Always all at once
            method = "all_at_once"
        elif self.mode == "B":
            # Mode B: Alternating - odd iterations = all_at_once, even = one_by_one
            method = "all_at_once" if iteration % 2 == 1 else "one_by_one"
        elif self.mode == "C":
            # Mode C: Random choice
            method = random.choice(["all_at_once", "one_by_one"])
        else:
            method = "all_at_once"  # Default fallback
        
        self.import_methods_used.append(method)
        return method
        
    def connect_database(self) -> DatabaseOperations:
        """Create and connect to database."""
        try:
            db_ops = DatabaseOperations(self.db_path)
            if not db_ops.connect():
                raise Exception("Failed to connect to database")
            return db_ops
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def diagnose_time_column(self, db_ops: DatabaseOperations):
        """Diagnose the Time column to understand its data type and sample values."""
        try:
            # Get table schema
            schema = db_ops.get_table_schema(self.target_table)
            time_column_info = None
            for col in schema:
                if col['column_name'].lower() == 'time':
                    time_column_info = col
                    break
            
            if time_column_info:
                logger.info(f"Time column info: {time_column_info}")
            else:
                logger.warning("Time column not found in schema")
            
            # Get sample values from Time column
            sample_query = f"SELECT TOP 10 [Time] FROM [{self.target_table}]"
            result = db_ops.execute_query(sample_query)
            
            logger.info("Sample Time column values:")
            for i, row in enumerate(result[:10]):
                logger.info(f"  Row {i+1}: {row[0]} (type: {type(row[0])})")
                
        except Exception as e:
            logger.error(f"Error diagnosing Time column: {e}")
    
    def count_2025_records(self, db_ops: DatabaseOperations) -> int:
        """Count records with Time year = 2025 (DateTime field)."""
        try:
            # Use Year function for DATETIME field (confirmed working from diagnostic)
            query = f"SELECT COUNT(*) FROM [{self.target_table}] WHERE Year([Time]) = 2025"
            result = db_ops.execute_query(query)
            return result[0][0] if result else 0
        except Exception as e:
            logger.error(f"Error counting 2025 records: {e}")
            return -1
    
    def delete_2025_records(self, db_ops: DatabaseOperations) -> Tuple[int, float]:
        """Delete all records with Time year = 2025 in batches. Returns (records_deleted, time_taken)."""
        start_time = time.time()
        try:
            # Count records before deletion
            initial_count = self.count_2025_records(db_ops)
            logger.info(f"Found {initial_count:,} records to delete")
            
            if initial_count == 0:
                return 0, time.time() - start_time
            
            # Delete in batches to avoid Access lock limit
            total_deleted = 0
            batch_size = 5000  # Safe batch size for Access
            batch_count = 0
            estimated_batches = (initial_count + batch_size - 1) // batch_size  # Ceiling division
            
            print(f"  Deleting {initial_count:,} records...", end="", flush=True)
            
            while True:
                # Delete a batch using TOP clause
                try:
                    delete_query = f"""
                    DELETE FROM [{self.target_table}] 
                    WHERE [ID] IN (
                        SELECT TOP {batch_size} [ID] 
                        FROM [{self.target_table}] 
                        WHERE Year([Time]) = 2025
                    )
                    """
                    
                    cursor = db_ops.connection.cursor()
                    cursor.execute(delete_query)
                    batch_deleted = cursor.rowcount
                    db_ops.connection.commit()
                    cursor.close()
                    
                    total_deleted += batch_deleted
                    batch_count += 1
                    
                    # Show progress only every 5 batches or at completion
                    if batch_count % 5 == 0 or batch_deleted < batch_size:
                        progress_percent = min(100, (total_deleted / initial_count) * 100)
                        print(f" {progress_percent:.0f}%", end="", flush=True)
                    
                    # Reduce logging frequency
                    if batch_count % 10 == 0 or batch_deleted < batch_size:
                        logger.info(f"Deleted batch {batch_count}: {batch_deleted:,} records (total: {total_deleted:,})")
                    
                    # Check if we're done
                    if batch_deleted < batch_size:
                        break
                        
                except Exception as batch_error:
                    logger.error(f"Batch delete error: {batch_error}")
                    raise
            
            # Verify deletion
            remaining_count = self.count_2025_records(db_ops)
            delete_time = time.time() - start_time
            
            if remaining_count > 0:
                logger.warning(f"Deletion incomplete: {remaining_count} records still remain")
            
            print(f" Done ({delete_time:.1f}s)")
            logger.info(f"Successfully deleted {total_deleted:,} records in {delete_time:.2f}s")
            return total_deleted, delete_time
            
        except Exception as e:
            delete_time = time.time() - start_time
            logger.error(f"Delete operation failed after {delete_time:.2f}s: {e}")
            raise
    
    def import_excel_files(self, db_ops: DatabaseOperations, method: str = "all_at_once") -> Tuple[int, float]:
        """
        Import Excel files using specified method.
        method: "all_at_once", "one_by_one", or "random"
        Returns (records_imported, time_taken)
        """
        start_time = time.time()
        total_imported = 0
        
        try:
            # Get table schema for data conversion
            table_schema = db_ops.get_table_schema(self.target_table)
            
            if method == "all_at_once":
                # Process all files in one operation
                all_records = []
                for excel_file in self.excel_files:
                    records = self._process_excel_file(excel_file, table_schema)
                    all_records.extend(records)
                
                if all_records:
                    total_imported = db_ops.insert_records_batch(self.target_table, all_records, batch_size=1000)
                    
            elif method in ["one_by_one", "random"]:
                # Process files individually
                for excel_file in self.excel_files:
                    records = self._process_excel_file(excel_file, table_schema)
                    if records:
                        imported = db_ops.insert_records_batch(self.target_table, records, batch_size=1000)
                        total_imported += imported
            
            import_time = time.time() - start_time
            logger.info(f"Successfully imported {total_imported} records using {method} method in {import_time:.2f}s")
            return total_imported, import_time
            
        except Exception as e:
            import_time = time.time() - start_time
            logger.error(f"Import operation failed after {import_time:.2f}s: {e}")
            raise
    
    def _process_excel_file(self, excel_file: str, table_schema: list) -> List[dict]:
        """Process a single Excel file and return converted records."""
        try:
            processor = ExcelProcessor(excel_file)
            excel_data = processor.read_sheet()
            
            converted_records = []
            for _, row in excel_data.iterrows():
                try:
                    record_dict = row.to_dict()
                    # Apply data type conversion
                    temp_db = DatabaseOperations(self.db_path)
                    record_dict = temp_db.convert_data_for_access(record_dict, table_schema)
                    if record_dict:
                        converted_records.append(record_dict)
                except Exception as e:
                    # Skip individual record errors
                    pass
            
            logger.info(f"Processed {excel_file}: {len(converted_records)} valid records")
            return converted_records
            
        except Exception as e:
            logger.error(f"Error processing {excel_file}: {e}")
            return []
    
    def verify_import(self, db_ops: DatabaseOperations, expected_records: int) -> bool:
        """Verify that the expected number of 2025 records were imported."""
        try:
            actual_count = self.count_2025_records(db_ops)
            if actual_count == expected_records:
                print(" OK")
                logger.info(f"Import verification passed: {actual_count} records")
                return True
            else:
                print(" FAIL")
                logger.error(f"Import verification failed: expected {expected_records}, found {actual_count}")
                return False
        except Exception as e:
            print(" ERROR")
            logger.error(f"Import verification error: {e}")
            return False
    
    def run_single_iteration(self, iteration: int) -> bool:
        """Run a single delete/import cycle. Returns True if successful."""
        import_method = self.get_import_method(iteration)
        logger.info(f"\n=== Starting Iteration {iteration}/{self.iterations} (Method: {import_method}) ===")
        
        try:
            db_ops = self.connect_database()
            
            # Step 0: Log expected record count on first iteration
            if iteration == 1:
                logger.info("Expected 2025 records to process: ~122,544 based on diagnostic")
            
            # Step 1: Count initial records
            initial_total = len(db_ops.read_table(self.target_table))
            initial_2025 = self.count_2025_records(db_ops)
            logger.info(f"Initial state: {initial_total:,} total records, {initial_2025:,} from 2025")
            
            # Step 2: Delete 2025 records
            deleted_count, delete_time = self.delete_2025_records(db_ops)
            
            # Step 3: Import records using selected method
            method_display = f"({import_method})" if self.mode != "A" else ""
            print(f"  Importing Excel files {method_display}...", end="", flush=True)
            imported_count, import_time = self.import_excel_files(db_ops, import_method)
            print(f" Done ({import_time:.1f}s)")
            
            # Step 4: Verify import
            print(f"  Verifying import...", end="", flush=True)
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
    
    def run_test(self):
        """Run the complete test cycle."""
        logger.info(f"Starting DataSync Import/Delete Cycle Test")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Excel files: {', '.join(self.excel_files)}")
        logger.info(f"Iterations: {self.iterations}")
        logger.info(f"Target table: {self.target_table}")
        
        start_time = time.time()
        successful_iterations = 0
        
        for i in range(1, self.iterations + 1):
            print(f"[{i}/{self.iterations}] ", end="", flush=True)
            
            success = self.run_single_iteration(i)
            if success:
                successful_iterations += 1
                print("[OK]")
            else:
                print("[FAIL]")
            
            self.stats['iterations_completed'] = i
        
        total_time = time.time() - start_time
        self._print_summary(total_time, successful_iterations)
    
    def _print_summary(self, total_time: float, successful_iterations: int):
        """Print comprehensive test summary with method analysis."""
        mode_names = {"A": "All at Once", "B": "Alternating", "C": "Random"}
        mode_name = mode_names.get(self.mode, "Unknown")
        
        print(f"\n{'='*60}")
        print(f"DATASYNC IMPORT/DELETE CYCLE TEST SUMMARY - MODE {self.mode}")
        print(f"{'='*60}")
        
        print(f"\nTEST CONFIGURATION:")
        print(f"  Database: {os.path.basename(self.db_path)}")
        print(f"  Excel files: {len(self.excel_files)} files")
        print(f"  Target table: {self.target_table}")
        print(f"  Test mode: {self.mode} ({mode_name})")
        print(f"  Planned iterations: {self.iterations}")
        
        print(f"\nOVERALL RESULTS:")
        print(f"  Total test time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        print(f"  Successful iterations: {successful_iterations}/{self.iterations}")
        print(f"  Success rate: {(successful_iterations/self.iterations)*100:.1f}%")
        print(f"  Failed iterations: {self.iterations - successful_iterations}")
        
        # Method usage analysis (for modes B and C)
        if self.mode != "A" and self.import_methods_used:
            method_counts = {}
            for method in self.import_methods_used:
                method_counts[method] = method_counts.get(method, 0) + 1
            
            print(f"\nIMPORT METHOD USAGE:")
            for method, count in method_counts.items():
                print(f"  {method}: {count} times ({count/len(self.import_methods_used)*100:.1f}%)")
            
            # Performance by method
            if len(self.stats['import_times']) == len(self.import_methods_used):
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
        
        # Save detailed log with mode-specific filename
        log_file = f"test_results_mode{self.mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(log_file, 'w') as f:
            f.write(f"DataSync Test Results Mode {self.mode} - {datetime.now()}\n")
            f.write(f"Import methods used: {self.import_methods_used}\n")
            f.write(f"Statistics: {self.stats}\n")
        print(f"Detailed results saved to: {log_file}")
        print(f"Debug logs saved to: test_import_cycle.log")

def get_user_input():
    """Get test configuration from user input."""
    print("🔧 DataSync Import/Delete Cycle Test Configuration")
    print("=" * 50)
    
    # Get test mode
    print("\nTest Modes:")
    print("  A - All at Once (import all files together every iteration)")
    print("  B - Alternating (alternate between 'all at once' and 'one by one')")
    print("  C - Random (randomly choose method each iteration)")
    
    while True:
        mode = input("\nSelect test mode (A/B/C) [default: A]: ").strip().upper()
        if not mode:
            mode = "A"
        if mode in ["A", "B", "C"]:
            break
        print("❌ Invalid mode. Please enter A, B, or C.")
    
    # Get iteration count
    while True:
        iterations_input = input(f"\nNumber of iterations [default: 10]: ").strip()
        if not iterations_input:
            iterations = 10
            break
        try:
            iterations = int(iterations_input)
            if iterations <= 0:
                print("❌ Please enter a positive number.")
                continue
            if iterations > 100:
                confirm = input(f"⚠️  {iterations} iterations may take a long time. Continue? (y/N): ").strip().lower()
                if confirm not in ['y', 'yes']:
                    continue
            break
        except ValueError:
            print("❌ Please enter a valid number.")
    
    return mode, iterations

def main():
    """Main function to run the test."""
    parser = argparse.ArgumentParser(description="DataSync Import/Delete Cycle Test")
    parser.add_argument("--mode", choices=["A", "B", "C"], 
                       help="Test mode: A=all at once, B=alternating, C=random")
    parser.add_argument("--iterations", type=int, 
                       help="Number of test iterations")
    parser.add_argument("--interactive", action="store_true",
                       help="Use interactive mode to configure test settings")
    args = parser.parse_args()
    
    # Determine if we should use interactive mode
    use_interactive = args.interactive or (args.mode is None and args.iterations is None)
    
    if use_interactive:
        # Interactive mode
        mode, iterations = get_user_input()
        print(f"\n✅ Configuration: Mode {mode}, {iterations} iterations")
    else:
        # Command line mode
        mode = args.mode or "A"
        iterations = args.iterations or 10
    
    # Configuration
    DATABASE_PATH = "working db 8.25.2025 prodjectDataPTP.accdb"
    EXCEL_FILES = [
        "data/8.18.25 - all other - Projected KPI's COT - TG Fixed Public - Default.xls",
        "data/8.18.25 - groc all other - Projected KPI's COT - TG Fixed Public - Default.xls", 
        "data/8.18.25 - grocery enlow - Projected KPI's COT - TG Fixed Public - Default.xls"
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
    if mode == "C":
        random.seed(42)
        print(f"🎲 Random seed set to 42 for reproducible results")
    
    # Run the test
    mode_names = {"A": "All at Once", "B": "Alternating", "C": "Random"}
    mode_name = mode_names.get(mode, "Unknown")
    print(f"🚀 Starting DataSync Import/Delete Cycle Test - Mode {mode} ({mode_name})")
    print(f"   Database: {os.path.basename(DATABASE_PATH)}")
    print(f"   Excel files: {len(EXCEL_FILES)} files")
    print(f"   Iterations: {iterations}")
    
    test = ImportCycleTest(DATABASE_PATH, EXCEL_FILES, iterations, mode)
    test.run_test()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
