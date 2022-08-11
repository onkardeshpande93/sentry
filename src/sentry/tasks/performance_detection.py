import hashlib
import random
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import sentry_sdk

from sentry import options
from sentry.eventstore.processing.base import Event
from sentry.utils import metrics

Span = Dict[str, Any]
TransactionSpans = List[Span]
PerformanceIssues = Dict[str, Any]


class DetectorType(Enum):
    SLOW_SPAN = "slow_span"
    DUPLICATE_SPANS = "duplicates"
    SEQUENTIAL_SLOW_SPANS = "sequential"


# Facade in front of performance detection to limit impact of detection on our events ingestion
def detect_performance_issue(data: Event):
    try:
        rate = options.get("store.use-ingest-performance-detection-only")
        if rate and rate > random.random():
            # Add an experimental tag to be able to find these spans in production while developing. Should be removed later.
            sentry_sdk.set_tag("_did_analyze_performance_issue", "true")
            with metrics.timer(
                "performance.detect_performance_issue", sample_rate=0.01
            ), sentry_sdk.start_span(
                op="py.detect_performance_issue", description="none"
            ) as sdk_span:
                _detect_performance_issue(data, sdk_span)
    except Exception as e:
        sentry_sdk.capture_exception(e)


# Gets some of the thresholds to perform performance detection. Can be made configurable later.
# Thresholds are in milliseconds.
def get_default_detection_settings():
    return {
        DetectorType.DUPLICATE_SPANS: {
            "count": 5,
            "cumulative_duration": 500.0,  # ms
            "allowed_span_ops": ["db", "http"],
        },
        DetectorType.SEQUENTIAL_SLOW_SPANS: {
            "count": 3,
            "cumulative_duration": 1200.0,  # ms
            "allowed_span_ops": ["db", "http", "ui"],
        },
        DetectorType.SLOW_SPAN: {
            "duration_threshold": 1000.0,  # ms
            "allowed_span_ops": ["db", "http"],
        },
    }


def _detect_performance_issue(data: Event, sdk_span: Any):
    event_id = data.get("event_id", None)
    spans: TransactionSpans = data.get("spans", [])

    detection_settings = get_default_detection_settings()
    detectors = {
        DetectorType.DUPLICATE_SPANS: DuplicateSpanDetector(detection_settings),
        DetectorType.SLOW_SPAN: SlowSpanDetector(detection_settings),
        DetectorType.SEQUENTIAL_SLOW_SPANS: SequentialSlowSpanDetector(detection_settings),
    }

    for span in spans:
        for _, detector in detectors.items():
            detector.visit_span(span)

    report_metrics_for_detectors(event_id, detectors, sdk_span)


# Creates a stable fingerprint given the same span details using sha1.
def fingerprint_span(span: Span):
    op = span.get("op", None)
    description = span.get("description", None)
    if not description or not op:
        return None

    signature = (str(op) + str(description)).encode("utf-8")
    full_fingerprint = hashlib.sha1(signature).hexdigest()
    fingerprint = full_fingerprint[
        :20
    ]  # 80 bits. Not a cryptographic usage, we don't need all of the sha1 for collision detection

    return fingerprint


# Simple fingerprint for broader checks, using the span op.
def fingerprint_span_op(span: Span):
    op = span.get("op", None)
    if not op:
        return None
    return op


def get_span_duration(span: Span):
    return timedelta(seconds=span.get("timestamp", 0)) - timedelta(
        seconds=span.get("start_timestamp", 0)
    )


class PerformanceSpanIssue:
    __slots__ = ("span_id", "allowed_op", "spans_involved")
    """
    A class representing a detected performance issue caused by a performance span
    """

    def __init__(self, span_id: str, allowed_op: str, spans_involved: List[str]):
        self.span_id = span_id
        self.allowed_op = allowed_op
        self.spans_involved = spans_involved


class PerformanceDetector(ABC):
    """
    Classes of this type have their visit functions called as the event is walked once and will store a performance issue if one is detected.
    """

    def __init__(self, settings: Dict[str, Any]):
        self.settings = settings[self.settings_key]
        self.init()

    @abstractmethod
    def init(self):
        raise NotImplementedError

    def span_op_allowed(self, span_op: str):
        allowed_span_ops = self.settings.get("allowed_span_ops", [])
        if len(allowed_span_ops) <= 0:
            return True
        return next((op for op in allowed_span_ops if span_op.startswith(op)), False)

    @property
    @abstractmethod
    def settings_key(self) -> DetectorType:
        raise NotImplementedError

    @abstractmethod
    def visit_span(self, span: Span) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def stored_issues(self) -> PerformanceIssues:
        raise NotImplementedError


class DuplicateSpanDetector(PerformanceDetector):
    """
    Broadly check for duplicate spans.
    """

    __slots__ = ("cumulative_durations", "duplicate_spans_involved", "stored_issues")

    settings_key = DetectorType.DUPLICATE_SPANS

    def init(self):
        self.cumulative_durations = {}
        self.duplicate_spans_involved = {}
        self.stored_issues = {}

    def visit_span(self, span: Span):
        op = span.get("op", None)
        span_id = span.get("span_id", None)
        if not op or not span_id:
            return

        fingerprint = fingerprint_span(span)

        duplicate_count_threshold = self.settings.get("count")
        duplicate_duration_threshold = self.settings.get("cumulative_duration")
        allowed_op = self.span_op_allowed(op)

        if not fingerprint or not allowed_op:
            return

        span_duration = get_span_duration(span)

        self.cumulative_durations[fingerprint] = (
            self.cumulative_durations.get(fingerprint, timedelta(0)) + span_duration
        )

        if fingerprint not in self.duplicate_spans_involved:
            self.duplicate_spans_involved[fingerprint] = []

        self.duplicate_spans_involved[fingerprint] += [span_id]
        duplicate_spans_counts = len(self.duplicate_spans_involved[fingerprint])

        if not self.stored_issues.get(fingerprint, False):
            if duplicate_spans_counts >= duplicate_count_threshold and self.cumulative_durations[
                fingerprint
            ] >= timedelta(milliseconds=duplicate_duration_threshold):
                spans_involved = self.duplicate_spans_involved[fingerprint]
                self.stored_issues[fingerprint] = PerformanceSpanIssue(
                    span_id, allowed_op, spans_involved
                )


class SlowSpanDetector(PerformanceDetector):
    """
    Check for slow spans in a certain type of span.op (eg. slow db spans)
    """

    __slots__ = "stored_issues"

    settings_key = DetectorType.SLOW_SPAN

    def init(self):
        self.stored_issues = {}

    def visit_span(self, span: Span):
        op = span.get("op", None)
        span_id = span.get("span_id", None)
        if not op or not span_id:
            return

        fingerprint = fingerprint_span(span)

        slow_span_duration_threshold = self.settings.get("duration_threshold")
        allowed_op = self.span_op_allowed(op)

        if not fingerprint or not allowed_op:
            return

        span_duration = get_span_duration(span)

        if span_duration >= timedelta(
            milliseconds=slow_span_duration_threshold
        ) and not self.stored_issues.get(fingerprint, False):
            spans_involved = [span_id]
            self.stored_issues[fingerprint] = PerformanceSpanIssue(
                span_id, allowed_op, spans_involved
            )


class SequentialSlowSpanDetector(PerformanceDetector):
    """
    Checks for unparallelized slower repeated spans, to suggest using futures etc. to reduce response time.
    This makes some assumptions about span ordering etc. and also removes any spans that have any overlap with the same span op from consideration.
    """

    __slots__ = ("cumulative_durations", "stored_issues", "spans_involved", "last_span_seen")

    settings_key = DetectorType.SEQUENTIAL_SLOW_SPANS

    def init(self):
        self.cumulative_durations = {}
        self.stored_issues = {}
        self.spans_involved = {}
        self.last_span_seen = {}

    def visit_span(self, span: Span):
        op = span.get("op", None)
        span_id = span.get("span_id", None)
        if not op or not span_id:
            return

        fingerprint = fingerprint_span_op(span)

        count_threshold = self.settings.get("count")
        duration_threshold = self.settings.get("cumulative_duration")
        allowed_op = self.span_op_allowed(op)

        if not fingerprint or not allowed_op:
            return

        span_duration = get_span_duration(span)
        span_end = timedelta(seconds=span.get("timestamp", 0))

        if fingerprint not in self.spans_involved:
            self.spans_involved[fingerprint] = []

        self.spans_involved[fingerprint] += [span_id]

        if fingerprint not in self.last_span_seen:
            self.last_span_seen[fingerprint] = span_end
            self.cumulative_durations[fingerprint] = span_duration
            return

        last_span_end = self.last_span_seen[fingerprint]
        current_span_start = timedelta(seconds=span.get("start_timestamp", 0))

        are_spans_overlapping = current_span_start <= last_span_end
        if are_spans_overlapping:
            del self.last_span_seen[fingerprint]
            self.spans_involved[fingerprint] = []
            self.cumulative_durations[fingerprint] = timedelta(0)
            return

        self.cumulative_durations[fingerprint] += span_duration
        self.last_span_seen[fingerprint] = span_end

        spans_counts = len(self.spans_involved[fingerprint])

        if not self.stored_issues.get(fingerprint, False):
            if spans_counts >= count_threshold and self.cumulative_durations[
                fingerprint
            ] >= timedelta(milliseconds=duration_threshold):
                spans_involved = self.spans_involved[fingerprint]
                self.stored_issues[fingerprint] = PerformanceSpanIssue(
                    span_id, allowed_op, spans_involved
                )


# Reports metrics and creates spans for detection
def report_metrics_for_detectors(
    event_id: Optional[str], detectors: Dict[str, PerformanceDetector], sdk_span: Any
):
    all_detected_issues = [i for _, d in detectors.items() for i in d.stored_issues]
    has_detected_issues = bool(all_detected_issues)

    if has_detected_issues:
        sdk_span.containing_transaction.set_tag("_pi_all_issue_count", len(all_detected_issues))
        metrics.incr(
            "performance.performance_issue.aggregate",
            len(all_detected_issues),
        )
        if event_id:
            sdk_span.containing_transaction.set_tag("_pi_transaction", event_id)

    detected_tags = {}
    for detector_enum, detector in detectors.items():
        detector_key = detector_enum.value
        detected_issues = detector.stored_issues
        detected_issue_keys = list(detected_issues.keys())
        detected_tags[detector_key] = bool(len(detected_issue_keys))

        if not detected_issue_keys:
            continue

        first_issue = detected_issues[detected_issue_keys[0]]
        sdk_span.containing_transaction.set_tag(f"_pi_{detector_key}", first_issue.span_id)
        metrics.incr(
            f"performance.performance_issue.{detector_key}",
            len(detected_issue_keys),
            tags={f"op_{n.allowed_op}": True for n in detected_issues.values()},
        )

    metrics.incr(
        "performance.performance_issue.detected",
        instance=str(has_detected_issues),
        tags=detected_tags,
    )
