package internal

import "testing"

func TestFormatPath_preservesHyphensPerSegment(t *testing.T) {
	got := formatPath([]string{"my-project", "foo_bar", "bux"})
	want := "my-project_foo_bar_bux"
	if got != want {
		t.Fatalf("formatPath(...) = %q, want %q", got, want)
	}
}
