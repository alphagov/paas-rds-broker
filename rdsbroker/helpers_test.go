package rdsbroker_test

func boolPointer(input bool) *bool {
	return &input
}
func int64Pointer(input int64) *int64 {
	return &input
}
func stringPointer(input string) *string {
	return &input
}

// worst. language. ever.
func copyStringStringMap(inMap map[string]string) map[string]string {
	outMap := map[string]string{}

	for key, value := range inMap {
		outMap[key] = value
	}

	return outMap
}
