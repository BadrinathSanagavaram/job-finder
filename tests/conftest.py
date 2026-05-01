"""Pytest configuration — shared fixtures and sys.path setup."""
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
