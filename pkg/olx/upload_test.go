package olx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// A file goes up as OLX's site sends one: a token from the chat API, the bytes
// to the upload service under that token, and then a message naming the file.
func TestUploadThenSendWithTheFile(t *testing.T) {
	var sent map[string]map[string]any
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/apollo/token":
			if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer good" {
				t.Errorf("token request: %s %q", r.Method, r.Header.Get("Authorization"))
			}
			fmt.Fprint(w, `{"data":{"token":"upload-token","expiry":1}}`)
		case "/v1/temp-files":
			body, _ := io.ReadAll(r.Body)
			expires, err := time.Parse(time.RFC3339, r.Header.Get("Expires"))
			if r.Header.Get("Authorization") != "Bearer upload-token" || r.Header.Get("Content-Type") != "image/png" ||
				string(body) != "bytes" || err != nil || time.Until(expires) < 23*time.Hour {
				t.Errorf("upload: auth %q type %q body %q expires %q", r.Header.Get("Authorization"),
					r.Header.Get("Content-Type"), body, r.Header.Get("Expires"))
			}
			fmt.Fprint(w, `{"data":{"filename":"stored-id"}}`)
		case "/api/conversations/c1/messages":
			_ = json.NewDecoder(r.Body).Decode(&sent)
			fmt.Fprint(w, `{"data":{}}`)
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	}, goodTokens())

	file, err := client.Upload(context.Background(), "photo.PNG", "image/png", []byte("bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if file.Filename != "photo.PNG" || file.FileID != "stored-id" {
		t.Errorf("uploaded %+v", file)
	}
	if _, err = client.SendMessage(context.Background(), "c1", "", file); err != nil {
		t.Fatal(err)
	}
	attachments, _ := sent["message"]["attachments"].([]any)
	if len(attachments) != 1 {
		t.Fatalf("sent %v", sent)
	}
	if got := attachments[0].(map[string]any); got["filename"] != "photo.PNG" || got["file_id"] != "stored-id" {
		t.Errorf("sent attachment %v", got)
	}
}

func TestOnlyKindsOLXTakesAreUploaded(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("nothing should be asked of OLX, got %s", r.URL.Path)
	}, goodTokens())
	if _, err := client.Upload(context.Background(), "clip.mp4", "video/mp4", []byte("x")); !errors.Is(err, ErrUnsendableFile) {
		t.Errorf("got %v", err)
	}
	for name, want := range map[string]bool{"a.jpg": true, "a.JPEG": true, "b.pdf": true, "c.xlsx": true, "d.gif": false, "noextension": false} {
		if CanSend(name) != want {
			t.Errorf("CanSend(%q) = %v", name, !want)
		}
	}
}

func TestAMessageWithoutFilesNamesNone(t *testing.T) {
	var sent map[string]map[string]any
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&sent)
		fmt.Fprint(w, `{"data":{}}`)
	}, goodTokens())
	if _, err := client.SendMessage(context.Background(), "c1", "Hej"); err != nil {
		t.Fatal(err)
	}
	if _, has := sent["message"]["attachments"]; has {
		t.Errorf("sent %v", sent)
	}
}
