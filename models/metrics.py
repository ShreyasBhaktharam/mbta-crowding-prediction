from __future__ import annotations

import numpy as np


def mae(y_true, y_pred) -> float:
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true, y_pred) -> float:
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    return float(np.sqrt(np.mean(np.square(y_true - y_pred))))


def pinball_loss(y_true, y_pred, quantile: float) -> float:
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    diff = y_true - y_pred
    return float(np.mean(np.maximum(quantile * diff, (quantile - 1) * diff)))


def coverage(y_true, lower, upper) -> float:
    y_true = np.array(y_true)
    lower = np.array(lower)
    upper = np.array(upper)
    within = np.logical_and(y_true >= lower, y_true <= upper)
    return float(np.mean(within))
