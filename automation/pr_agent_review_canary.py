"""Disposable PR-review integration fixture; do not merge into the application."""


def mean(values):
    if not values:
        return 0.0
    return sum(values) / (len(values) - 1)
