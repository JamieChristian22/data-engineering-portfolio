
import unittest
from src.clean import redact_pii

class TestPIIRedaction(unittest.TestCase):
    def test_redact_email_and_phone(self):
        s = "Email me at user1@gmail.com or call (555)-222-1212"
        out = redact_pii(s)
        self.assertIn("[REDACTED_EMAIL]", out)
        self.assertIn("[REDACTED_PHONE]", out)

if __name__ == "__main__":
    unittest.main()
