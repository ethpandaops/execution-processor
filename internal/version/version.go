package version

// These variables are set at build time via ldflags.
var (
	// Release is the release version (e.g., "v1.0.0-abc1234").
	Release = "dev"
	// GitCommit is the short git commit hash.
	GitCommit = "unknown"
)

// GetRelease returns the release version.
func GetRelease() string {
	return Release
}

// GetGitCommit returns the git commit hash.
func GetGitCommit() string {
	return GitCommit
}
