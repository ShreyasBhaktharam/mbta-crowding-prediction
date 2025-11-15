import numpy as np
import pytest

from models import metrics


def test_metric_functions():
    y_true = np.array([0.0, 1.0, 2.0])
    y_pred = np.array([0.0, 2.0, 1.0])
    assert metrics.mae(y_true, y_pred) == 1.0
    assert metrics.rmse(y_true, y_pred) == pytest.approx(1.291, rel=1e-2)
    assert metrics.pinball_loss(y_true, y_pred, 0.5) >= 0
    assert 0 <= metrics.coverage(y_true, y_pred - 1, y_pred + 1) <= 1
