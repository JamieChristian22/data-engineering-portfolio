
import unittest
from pathlib import Path
import pandas as pd

from src.features import build_session_features

class TestFeatures(unittest.TestCase):
    def test_build_session_features_label(self):
        df = pd.DataFrame([
            {"event_id":"1","user_id":1,"session_id":10,"event_time":"2025-01-01T00:00:00","event_type":"page_view","device":"mobile","referrer":"google","country":"US","event_index":0},
            {"event_id":"2","user_id":1,"session_id":10,"event_time":"2025-01-01T00:00:05","event_type":"purchase","device":"mobile","referrer":"google","country":"US","event_index":1},
            {"event_id":"3","user_id":2,"session_id":11,"event_time":"2025-01-01T00:00:00","event_type":"page_view","device":"desktop","referrer":"direct","country":"US","event_index":0},
        ])
        feats = build_session_features(df)
        label_map = dict(zip(feats["session_id"], feats["label_purchase"]))
        self.assertEqual(label_map[10], 1)
        self.assertEqual(label_map[11], 0)

if __name__ == "__main__":
    unittest.main()
