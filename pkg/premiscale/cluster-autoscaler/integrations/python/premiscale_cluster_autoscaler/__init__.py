"""
Integrate Cluster Autoscaler's external gRPC provider with PremiScale.
"""

from .runtime import KubernetesAutoscaler, serve

__all__ = ['KubernetesAutoscaler', 'serve']
