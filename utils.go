package gitd

import (
	"path/filepath"
	"strings"
)

// getContentType returns the MIME type based on file extension
func getContentType(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "application/javascript; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".xml":
		return "application/xml; charset=utf-8"
	case ".md":
		return "text/markdown; charset=utf-8"
	case ".txt":
		return "text/plain; charset=utf-8"
	case ".go":
		return "text/x-go; charset=utf-8"
	case ".py":
		return "text/x-python; charset=utf-8"
	case ".java":
		return "text/x-java; charset=utf-8"
	case ".c", ".h":
		return "text/x-c; charset=utf-8"
	case ".cpp", ".hpp", ".cc":
		return "text/x-c++; charset=utf-8"
	case ".rs":
		return "text/x-rust; charset=utf-8"
	case ".ts":
		return "text/typescript; charset=utf-8"
	case ".tsx":
		return "text/tsx; charset=utf-8"
	case ".jsx":
		return "text/jsx; charset=utf-8"
	case ".yaml", ".yml":
		return "text/yaml; charset=utf-8"
	case ".sh", ".bash":
		return "text/x-shellscript; charset=utf-8"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	case ".pdf":
		return "application/pdf"
	case ".zip":
		return "application/zip"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	default:
		return "text/plain; charset=utf-8"
	}
}
