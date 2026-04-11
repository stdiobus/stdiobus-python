"""Full coverage tests for errors.py — all error classes and error_from_code."""

import pytest
from stdiobus.errors import (
    ErrorCode,
    StdioBusError,
    InvalidArgumentError,
    InvalidStateError,
    TimeoutError,
    CancelledError,
    TransportError,
    NegotiationFailedError,
    PolicyDeniedError,
    UnavailableError,
    ResourceExhaustedError,
    NotSupportedError,
    InternalError,
    error_from_code,
)


class TestErrorCode:

    def test_all_codes(self):
        assert ErrorCode.INVALID_ARGUMENT == 1
        assert ErrorCode.INVALID_STATE == 2
        assert ErrorCode.TIMEOUT == 3
        assert ErrorCode.CANCELLED == 4
        assert ErrorCode.TRANSPORT_ERROR == 5
        assert ErrorCode.NEGOTIATION_FAILED == 6
        assert ErrorCode.POLICY_DENIED == 7
        assert ErrorCode.UNAVAILABLE == 8
        assert ErrorCode.RESOURCE_EXHAUSTED == 9
        assert ErrorCode.NOT_SUPPORTED == 10
        assert ErrorCode.INTERNAL == 99


class TestAllErrorClasses:
    """Verify every error class has correct code and inherits from StdioBusError."""

    @pytest.mark.parametrize("cls,code", [
        (InvalidArgumentError, 1),
        (InvalidStateError, 2),
        (TimeoutError, 3),
        (CancelledError, 4),
        (TransportError, 5),
        (NegotiationFailedError, 6),
        (PolicyDeniedError, 7),
        (UnavailableError, 8),
        (ResourceExhaustedError, 9),
        (NotSupportedError, 10),
        (InternalError, 99),
    ])
    def test_error_class(self, cls, code):
        assert issubclass(cls, StdioBusError)
        err = cls("test message")
        assert err.code == code
        assert err.message == "test message"
        assert err.details is None

    @pytest.mark.parametrize("cls", [
        InvalidArgumentError, InvalidStateError, TimeoutError,
        CancelledError, TransportError, NegotiationFailedError,
        PolicyDeniedError, UnavailableError, ResourceExhaustedError,
        NotSupportedError, InternalError,
    ])
    def test_error_with_details(self, cls):
        err = cls("msg", details={"key": "val"})
        assert err.details == {"key": "val"}
        d = err.to_dict()
        assert d["details"] == {"key": "val"}

    @pytest.mark.parametrize("cls", [
        InvalidArgumentError, InvalidStateError, TimeoutError,
        CancelledError, TransportError, NegotiationFailedError,
        PolicyDeniedError, UnavailableError, ResourceExhaustedError,
        NotSupportedError, InternalError,
    ])
    def test_error_str(self, cls):
        err = cls("test")
        s = str(err)
        assert "test" in s
        assert err.code.name in s


class TestErrorFromCode:

    @pytest.mark.parametrize("code,expected_cls", [
        (1, InvalidArgumentError),
        (2, InvalidStateError),
        (3, TimeoutError),
        (4, CancelledError),
        (5, TransportError),
        (6, NegotiationFailedError),
        (7, PolicyDeniedError),
        (8, UnavailableError),
        (9, ResourceExhaustedError),
        (10, NotSupportedError),
        (99, InternalError),
    ])
    def test_known_codes(self, code, expected_cls):
        err = error_from_code(code, "msg")
        assert isinstance(err, expected_cls)
        assert err.message == "msg"

    def test_unknown_code_returns_internal(self):
        err = error_from_code(999, "unknown")
        assert isinstance(err, InternalError)
        assert err.message == "unknown"

    def test_negative_code_returns_internal(self):
        err = error_from_code(-1, "negative")
        assert isinstance(err, InternalError)

    def test_with_details(self):
        err = error_from_code(3, "timeout", details={"ms": 5000})
        assert isinstance(err, TimeoutError)
        assert err.details == {"ms": 5000}

    def test_to_dict_without_details(self):
        err = error_from_code(1, "bad arg")
        d = err.to_dict()
        assert d == {"code": 1, "message": "bad arg"}
        assert "details" not in d
