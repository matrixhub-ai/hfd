package authenticate

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	xettoken "github.com/wzshiming/xet/token"
)

// xetTokenTTL bounds the life of minted CAS tokens.
const xetTokenTTL = 60 * time.Minute

// xetTokenMethod and xetTokenPath are the fixed signing scope of CAS
// tokens: one grant covers the whole CAS surface. The slash makes the
// method an invalid HTTP token, so no wire request can ever replay a CAS
// token through the method+URL-bound user auth chain, and vice versa.
const (
	xetTokenMethod = "xet/cas"
	xetTokenPath   = "/"
)

// NewXETTokenScheme returns the CAS token mint/validate pair: hfd's signed
// token mechanism when a validator is injected, otherwise a process-local
// random-key issuer.
func NewXETTokenScheme(v TokenSignValidator) (mint func(time.Time) (token string, exp int64), authFn func(string) bool, err error) {
	if v != nil {
		mint = func(now time.Time) (string, int64) {
			tok, err := v.Sign(context.Background(), xetTokenMethod, xetTokenPath, "xet-cas", xetTokenTTL)
			if err != nil || tok == "" {
				// Fail closed: an empty token cannot pass the CAS AuthFunc.
				slog.Error("mint CAS token", "error", err)
				return "", 0
			}
			return tok, now.Add(xetTokenTTL).Unix()
		}
		authFn = func(tok string) bool {
			_, _, ok, err := v.Validate(context.Background(), xetTokenMethod, xetTokenPath, tok)
			return err == nil && ok
		}
		return mint, authFn, nil
	}
	issuer, err := xettoken.NewIssuer(nil, xetTokenTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("create token issuer: %w", err)
	}
	return issuer.Mint, func(tok string) bool { return issuer.Validate(tok, time.Now()) }, nil
}
