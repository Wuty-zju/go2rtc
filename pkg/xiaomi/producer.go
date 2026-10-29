package xiaomi

import (
	"errors"
	"net/url"
	"strings"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/legacy"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/miss"
)

func Dial(rawURL string) (core.Producer, error) {
	// Format: xiaomi/miss
	if strings.Contains(rawURL, "vendor") {
		return miss.Dial(rawURL)
	}

	// Format: xiaomi/legacy
	if u, err := url.Parse(rawURL); err == nil && u.Query().Has("ptime") {
		return nil, errors.New("xiaomi: ptime requires a MISS camera")
	}
	return legacy.Dial(rawURL)
}

func IsLegacy(model string) bool {
	return legacy.Supported(model)
}
