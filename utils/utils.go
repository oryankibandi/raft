package utils

/*
Removes control characters (1 - 31 ASCII)
*/
func RemoveControlChar(data []byte) []byte {
	newSeq := make([]byte, len(data))

	for _, v := range data {
		if v > byte(31) {
			newSeq = append(newSeq, v)
		}
	}

	return newSeq
}
