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

// copyStringStringMap ensures we copy the map, instead of the reference to the map.
// apparently copying a map is "such an uncommon operation" it's ok to require a
// loop for this in go land.
func copyStringStringMap(inMap map[string]string) map[string]string {
	outMap := map[string]string{}

	for key, value := range inMap {
		outMap[key] = value
	}

	return outMap
}
