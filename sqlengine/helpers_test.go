package sqlengine

import "encoding/json"

func boolPointer(input bool) *bool {
	return &input
}
func int64Pointer(input int64) *int64 {
	return &input
}
func stringPointer(input string) *string {
	return &input
}

func rawMessagePointer(input string) *json.RawMessage {
	x := json.RawMessage(input)
	return &x
}
