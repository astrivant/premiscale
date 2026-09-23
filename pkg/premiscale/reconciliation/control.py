"""
Bound collection concurrency using smoothed throughput and PID feedback.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import CollectionControl
    from premiscale.schemas.collection import CollectionReport


class ThroughputController:
    """
    Adjust a worker's thread count only after its current pass has completed.
    """

    def __init__(self, settings: CollectionControl, hosts: int, maximum: int, interval: float) -> None:
        """
        Establish per-worker bounds and a successful-hosts-per-second target.

        Args:
            settings (CollectionControl): Gains, smoothing, and concurrency settings.
            hosts (int): Number of hosts assigned to this worker.
            maximum (int): Configured maximum simultaneous host connections per worker.
            interval (float): Desired seconds between collection passes.

        Raises:
            ValueError: If the partition, bounds, or collection interval are invalid.
        """
        if hosts < 1 or maximum < settings.initialThreads or interval <= 0:
            raise ValueError('Invalid collection partition or controller bounds')
        self.settings = settings
        self.minimum = min(settings.minThreads, hosts)
        self.maximum = min(maximum, hosts)
        self.initial = min(settings.initialThreads, hosts)
        self.threads = self.initial
        self.target = settings.targetThroughput or hosts / interval
        self.interval = interval
        self.observed: float | None = None
        self.integral = 0.0
        self.previous_error: float | None = None

    def update(self, report: CollectionReport) -> int:
        """
        Apply bounded PID feedback with smoothing, deadband, and anti-windup.

        Failed hosts freeze concurrency and clear accumulated error, so outages
        cannot drive connection growth. Thread pools adopt the result next pass.

        Args:
            report (CollectionReport): Completed pass timing and successful-host counts.

        Returns:
            int: Thread count for the next collection pass.
        """
        if not report.attempted or report.succeeded != report.attempted:
            self.integral = 0.0
            self.previous_error = None
            self.observed = None
            return self.threads
        rate = report.throughput
        weight = self.settings.smoothing
        self.observed = rate if self.observed is None else weight * rate + (1 - weight) * self.observed
        error = (self.target - self.observed) / self.target
        elapsed = max(report.elapsed, self.interval)
        derivative = 0.0 if self.previous_error is None else (error - self.previous_error) / elapsed
        self.previous_error = error
        if abs(error) <= self.settings.deadband:
            return self.threads
        gain = self.settings.integralGain
        bound = (self.maximum - self.minimum + 1) / gain if gain else 0.0
        candidate = max(-bound, min(bound, self.integral + error * elapsed))
        proportional = self.settings.proportionalGain * error
        differential = self.settings.derivativeGain * derivative
        output = self.initial + proportional + gain * candidate + differential
        # Integrate only while unsaturated or when error moves away from a limit.
        if not ((output > self.maximum and error > 0) or (output < self.minimum and error < 0)):
            self.integral = candidate
        desired = max(self.minimum, min(self.maximum, round(output)))
        step = self.settings.maxStep
        self.threads = max(self.threads - step, min(self.threads + step, desired))
        return self.threads
