package numpool

import (
	"os"
	"testing"

	"github.com/yuku/numpool/internal"
)

func TestMain(m *testing.M) {
	internal.SetupTestDatabase()
	os.Exit(m.Run())
}
