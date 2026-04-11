"""Native FFI bindings for libstdio_bus.

To build the native extension:
    cd sdk/python
    python -m stdiobus._native.build_ffi

Prerequisites:
    - cffi package: pip install cffi
    - libstdio_bus.a built: cd ../.. && make lib
"""

try:
    from stdiobus._native._ffi import ffi, lib
    AVAILABLE = True
except ImportError:
    ffi = None
    lib = None
    AVAILABLE = False

__all__ = ['ffi', 'lib', 'AVAILABLE']
