package mirror

// Spool publication seams, exposed so ingest_test.go can record their order and fail each step.
var (
	OpenSpoolFile = &openSpoolFile
	SyncSpoolFile = &syncSpoolFile
	RenameSpool   = &renameSpool
	SyncSpoolDir  = &syncSpoolDir
)
