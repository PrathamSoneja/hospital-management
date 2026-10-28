"""
Uniform REST error handling and exception converters for FastAPI.
Guarantees consistent JSON error response schema across all services.
"""
from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

class AppException(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        retryable: bool = False,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retryable = retryable
        self.details = details or {}

def register_error_handlers(app: FastAPI, service_name: str):
    @app.exception_handler(AppException)
    async def app_exception_handler(request: Request, exc: AppException):
        request_id = getattr(request.state, "request_id", request.headers.get("X-Request-ID", "unknown"))
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "request_id": request_id,
                    "retryable": exc.retryable,
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        request_id = getattr(request.state, "request_id", request.headers.get("X-Request-ID", "unknown"))
        # Format validation error messages cleanly
        error_msgs = []
        for err in exc.errors():
            loc = ".".join(str(p) for p in err.get("loc", []))
            msg = err.get("msg", "Invalid value")
            error_msgs.append(f"{loc}: {msg}" if loc else msg)
        formatted_message = "; ".join(error_msgs) if error_msgs else "Invalid request payload"
        
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "error": {
                    "code": "INVALID_ARGUMENT",
                    "message": formatted_message,
                    "request_id": request_id,
                    "retryable": False,
                }
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        request_id = getattr(request.state, "request_id", request.headers.get("X-Request-ID", "unknown"))
        code_map = {
            status.HTTP_400_BAD_REQUEST: "INVALID_ARGUMENT",
            status.HTTP_404_NOT_FOUND: "NOT_FOUND",
            status.HTTP_409_CONFLICT: "FAILED_PRECONDITION",
            status.HTTP_503_SERVICE_UNAVAILABLE: "UNAVAILABLE",
            status.HTTP_504_GATEWAY_TIMEOUT: "DEADLINE_EXCEEDED",
        }
        code = code_map.get(exc.status_code, "INTERNAL_ERROR")
        retryable = exc.status_code in (status.HTTP_503_SERVICE_UNAVAILABLE, status.HTTP_504_GATEWAY_TIMEOUT)
        
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": code,
                    "message": str(exc.detail),
                    "request_id": request_id,
                    "retryable": retryable,
                }
            },
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        request_id = getattr(request.state, "request_id", request.headers.get("X-Request-ID", "unknown"))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": f"An unexpected internal error occurred: {str(exc)}",
                    "request_id": request_id,
                    "retryable": False,
                }
            },
        )
