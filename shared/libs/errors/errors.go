package errors

import (
	"errors"
	"fmt"
	"net/http"
)

type AppError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
	Err     error  `json:"-"`
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *AppError) Unwrap() error {
	return e.Err
}

func New(code, message string, status int) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Status:  status,
	}
}

func Wrap(err error, code, message string, status int) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Status:  status,
		Err:     err,
	}
}

var (
	ErrNotFound         = New("NOT_FOUND", "Resource not found", http.StatusNotFound)
	ErrBadRequest       = New("BAD_REQUEST", "Invalid request", http.StatusBadRequest)
	ErrUnauthorized     = New("UNAUTHORIZED", "Unauthorized access", http.StatusUnauthorized)
	ErrForbidden        = New("FORBIDDEN", "Access forbidden", http.StatusForbidden)
	ErrInternal         = New("INTERNAL_ERROR", "Internal server error", http.StatusInternalServerError)
	ErrConflict         = New("CONFLICT", "Resource conflict", http.StatusConflict)
	ErrValidation       = New("VALIDATION_ERROR", "Validation failed", http.StatusUnprocessableEntity)
	ErrServiceUnavailable = New("SERVICE_UNAVAILABLE", "Service temporarily unavailable", http.StatusServiceUnavailable)
)

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	return errors.As(err, target)
}
