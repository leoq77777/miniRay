"""Accounting guards prevent rejected/lost tasks from being reported as successful throughput."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'benchmarks'))
from run_suite import cases, validate
from summarize import distribution


class BenchmarkAccounting(unittest.TestCase):
    def setUp(self):
        self.case = next(case for case in cases(20) if case['scenario'] == 'scaling')
        self.row = dict(tasks=20, accepted=20, rejected=0, completed=20, failed=0, p50_ms=1,
                        p95_ms=2, p99_ms=3, elapsed_s=1, attempts=20)

    def test_healthy_batch_and_dropped_completion(self):
        validate(self.row, self.case)
        self.row['completed'] = 19
        with self.assertRaises(RuntimeError):
            validate(self.row, self.case)

    def test_overload_must_contain_rejections_and_completions(self):
        self.case['scenario'] = 'overload'
        with self.assertRaises(RuntimeError):
            validate(self.row, self.case)
        self.row.update(accepted=8, rejected=12, completed=8, attempts=8)
        validate(self.row, self.case)

    def test_nonfinite_or_unexpected_retries_are_rejected(self):
        self.row['p99_ms'] = float('nan')
        with self.assertRaises(RuntimeError):
            validate(self.row, self.case)
        self.row.update(p99_ms=3, attempts=21)
        with self.assertRaises(RuntimeError):
            validate(self.row, self.case)

    def test_case_inventory_and_summary(self):
        inventory = cases(400)
        self.assertEqual(len({case['case_id'] for case in inventory}), 42)
        for case in inventory:
            if case['scenario'] == 'scaling':
                self.assertEqual(set(case['capacities']), {2})
        self.assertEqual(distribution([1, 1, 2, 3, 100])['median'], 2)
        self.assertEqual(distribution([1, 1, 2, 3, 100])['iqr'], 2)

    def test_failure_requires_retry_and_ordered_observations(self):
        self.case['scenario'] = 'failure'
        self.row.update(attempts=21, recovered_tasks=1, detection_observed_ms=500,
                        reassignment_observed_ms=600, recovery_observed_ms=850)
        validate(self.row, self.case)
        self.row['recovered_tasks'] = 0
        with self.assertRaises(RuntimeError):
            validate(self.row, self.case)


if __name__ == '__main__':
    unittest.main()
