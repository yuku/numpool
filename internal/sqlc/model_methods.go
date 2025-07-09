package sqlc

// FindUnusedResourceIndex finds the first unused resource index by examining the resource usage bitmap.
// Returns the index of the first free resource, or -1 if no resources are available.
func (n *Numpool) FindUnusedResourceIndex() int {
	for i := 0; i < int(n.MaxResourcesCount); i++ {
		if !isBitSet(n.ResourceUsageStatus.Bytes, i) {
			return i
		}
	}
	return -1
}

// isBitSet checks if the bit at the given position is set in the byte array
func isBitSet(bytes []byte, position int) bool {
	if position < 0 || position >= len(bytes)*8 {
		return false
	}

	byteIndex := position / 8
	bitIndex := position % 8

	// PostgreSQL stores bits in big-endian order within each byte
	return bytes[byteIndex]&(1<<(7-bitIndex)) != 0
}
