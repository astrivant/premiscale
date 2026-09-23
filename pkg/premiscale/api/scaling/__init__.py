"""
Expose aggregate worker demand to KEDA's Metrics API scaler.
"""

from .routes import create_blueprint

__all__ = ['create_blueprint']
