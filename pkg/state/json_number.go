package state

import (
	"encoding/json"
	"strconv"
)

// JSONInt64 handles JSON numbers that might be strings or numbers.
type JSONInt64 int64

// UnmarshalJSON implements json.Unmarshaler.
func (j *JSONInt64) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as number first
	var num int64
	if err := json.Unmarshal(data, &num); err == nil {
		*j = JSONInt64(num)

		return nil
	}

	// Try to unmarshal as string
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	// Parse string to int64
	num, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return err
	}

	*j = JSONInt64(num)

	return nil
}

// MarshalJSON implements json.Marshaler.
func (j JSONInt64) MarshalJSON() ([]byte, error) {
	return json.Marshal(int64(j))
}

// Int64 returns the value as int64.
func (j JSONInt64) Int64() int64 {
	return int64(j)
}

// JSONInt handles JSON numbers that might be strings or numbers.
type JSONInt int

// UnmarshalJSON implements json.Unmarshaler.
func (j *JSONInt) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as number first
	var num int
	if err := json.Unmarshal(data, &num); err == nil {
		*j = JSONInt(num)

		return nil
	}

	// Try to unmarshal as string
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	// Parse string to int
	num, err := strconv.Atoi(str)
	if err != nil {
		return err
	}

	*j = JSONInt(num)

	return nil
}

// MarshalJSON implements json.Marshaler.
func (j JSONInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(int(j))
}

// Int returns the value as int.
func (j JSONInt) Int() int {
	return int(j)
}

// JSONUint64 handles JSON numbers that might be strings or numbers (unsigned).
type JSONUint64 uint64

// UnmarshalJSON implements json.Unmarshaler.
func (j *JSONUint64) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as number first
	var num uint64
	if err := json.Unmarshal(data, &num); err == nil {
		*j = JSONUint64(num)

		return nil
	}

	// Try to unmarshal as string
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	// Parse string to uint64
	num, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return err
	}

	*j = JSONUint64(num)

	return nil
}

// MarshalJSON implements json.Marshaler.
func (j JSONUint64) MarshalJSON() ([]byte, error) {
	return json.Marshal(uint64(j))
}

// Uint64 returns the value as uint64.
func (j JSONUint64) Uint64() uint64 {
	return uint64(j)
}
