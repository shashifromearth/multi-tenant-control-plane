"""Wire contracts shared by the control plane and the worker simulator.

This package is the *only* code both processes share: message schemas and broker
topology. Keeping it tiny and dependency-light makes the coupling explicit and reviewable.
"""
