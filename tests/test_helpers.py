# tests/test_helpers.py
from etl.helpers import is_valid_email, extract_domain_from_email, assert_valid_email_examples

def test_is_valid_email():
    assert is_valid_email("a@b.com")
    assert not is_valid_email("abc")
    assert extract_domain_from_email("user@acme.com") == "acme.com"

def test_email_examples():
    assert_valid_email_examples()