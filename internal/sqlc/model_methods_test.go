package sqlc

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestFindUnusedResourceIndex(t *testing.T) {
	tests := []struct {
		name              string
		resourceUsage     []byte
		maxResourcesCount int32
		expectedIndex     int
	}{
		{
			name:              "all resources free",
			resourceUsage:     []byte{0x00}, // 00000000
			maxResourcesCount: 8,
			expectedIndex:     0,
		},
		{
			name:              "first resource used",
			resourceUsage:     []byte{0x80}, // 10000000
			maxResourcesCount: 8,
			expectedIndex:     1,
		},
		{
			name:              "first two resources used",
			resourceUsage:     []byte{0xC0}, // 11000000
			maxResourcesCount: 8,
			expectedIndex:     2,
		},
		{
			name:              "alternating resources used",
			resourceUsage:     []byte{0xAA}, // 10101010
			maxResourcesCount: 8,
			expectedIndex:     1,
		},
		{
			name:              "all resources used",
			resourceUsage:     []byte{0xFF}, // 11111111
			maxResourcesCount: 8,
			expectedIndex:     -1,
		},
		{
			name:              "max resources less than byte size",
			resourceUsage:     []byte{0xE0}, // 11100000
			maxResourcesCount: 3,
			expectedIndex:     -1,
		},
		{
			name:              "multi-byte - all free",
			resourceUsage:     []byte{0x00, 0x00}, // 00000000 00000000
			maxResourcesCount: 16,
			expectedIndex:     0,
		},
		{
			name:              "multi-byte - first byte full",
			resourceUsage:     []byte{0xFF, 0x00}, // 11111111 00000000
			maxResourcesCount: 16,
			expectedIndex:     8,
		},
		{
			name:              "multi-byte - specific pattern",
			resourceUsage:     []byte{0xFF, 0x80}, // 11111111 10000000
			maxResourcesCount: 16,
			expectedIndex:     9,
		},
		{
			name:              "PostgreSQL bit format - first resource used",
			resourceUsage:     []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // First byte has MSB set
			maxResourcesCount: 2,
			expectedIndex:     1, // First resource is used, second is free
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			numpool := &Numpool{
				MaxResourcesCount: tt.maxResourcesCount,
				ResourceUsageStatus: pgtype.Bits{
					Bytes: tt.resourceUsage,
					Len:   int32(len(tt.resourceUsage) * 8),
					Valid: true,
				},
			}

			index := numpool.FindUnusedResourceIndex()
			assert.Equal(t, tt.expectedIndex, index, "unexpected resource index")
		})
	}
}

func TestIsBitSet(t *testing.T) {
	tests := []struct {
		name     string
		bytes    []byte
		position int
		expected bool
	}{
		{
			name:     "first bit set",
			bytes:    []byte{0x80}, // 10000000
			position: 0,
			expected: true,
		},
		{
			name:     "first bit not set",
			bytes:    []byte{0x7F}, // 01111111
			position: 0,
			expected: false,
		},
		{
			name:     "last bit of first byte set",
			bytes:    []byte{0x01}, // 00000001
			position: 7,
			expected: true,
		},
		{
			name:     "bit in second byte",
			bytes:    []byte{0x00, 0x80}, // 00000000 10000000
			position: 8,
			expected: true,
		},
		{
			name:     "out of bounds negative",
			bytes:    []byte{0xFF},
			position: -1,
			expected: false,
		},
		{
			name:     "out of bounds positive",
			bytes:    []byte{0xFF},
			position: 8,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isBitSet(tt.bytes, tt.position)
			assert.Equal(t, tt.expected, result)
		})
	}
}
