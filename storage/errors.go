package storage

import "errors"

var (
	// ErrStorageClosed is returned when operating on a closed storage
	ErrStorageClosed = errors.New("storage is closed")

	// ErrSubscriptionClosed is returned when operating on a closed subscription
	ErrSubscriptionClosed = errors.New("subscription is closed")

	// ErrMessageNotFound is returned when a message cannot be found
	ErrMessageNotFound = errors.New("message not found")
)
