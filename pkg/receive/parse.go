package receive

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ParseHookOutput reads ref-update lines from the file written by the
// post-receive hook script.  Each line has the format:
//
//	<old-sha> <new-sha> <refname>
//
// It returns nil, nil when the file does not exist (i.e. the hook was
// never triggered, which is valid for empty pushes).
func ParseHookOutput(path string) ([]RefUpdate, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("receive: opening hook output: %w", err)
	}
	defer f.Close()

	var updates []RefUpdate
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("receive: malformed hook output line: %q", line)
		}
		updates = append(updates, RefUpdate{
			OldRev:  parts[0],
			NewRev:  parts[1],
			RefName: parts[2],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("receive: reading hook output: %w", err)
	}
	return updates, nil
}
