package olx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"
)

// uploadsURL is where OLX's site puts a file before sending it in a message. The
// site has the one address for all its countries.
const uploadsURL = "https://ireland.apollo.olxcdn.com/v1/temp-files"

// The kinds of file OLX's site lets you attach, by extension.
var sendableExtensions = []string{"jpg", "jpeg", "png", "doc", "pdf", "xls", "docx", "xlsx"}

// ErrUnsendableFile is a file of a kind OLX doesn't take.
var ErrUnsendableFile = errors.New("OLX only takes JPEG and PNG pictures, PDFs, and Word and Excel files")

// CanSend reports whether OLX takes a file with this name.
func CanSend(filename string) bool {
	extension := strings.ToLower(strings.TrimPrefix(path.Ext(filename), "."))
	for _, allowed := range sendableExtensions {
		if extension == allowed {
			return true
		}
	}
	return false
}

// SentAttachment is a file that's been uploaded, ready to go in a message.
type SentAttachment struct {
	Filename string `json:"filename"`
	FileID   string `json:"file_id"`
}

// Upload puts a file where OLX can attach it to a message, as its site does: it
// asks the chat API for a token for the upload service, and posts the bytes
// there. The file is kept for a day unless a message takes it.
func (c *Client) Upload(ctx context.Context, filename, mimeType string, data []byte) (SentAttachment, error) {
	if !CanSend(filename) {
		return SentAttachment{}, ErrUnsendableFile
	}
	var token struct {
		Token string `json:"token"`
	}
	if err := c.call(ctx, http.MethodPost, "api/apollo/token", nil, nil, &token); err != nil {
		return SentAttachment{}, err
	} else if token.Token == "" {
		return SentAttachment{}, errors.New("OLX gave no token to upload with")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.UploadsURL, bytes.NewReader(data))
	if err != nil {
		return SentAttachment{}, err
	}
	// The upload token goes only to the upload service, never the session's.
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Content-Type", mimeType)
	request.Header.Set("Expires", time.Now().Add(24*time.Hour).UTC().Format("2006-01-02T15:04:05.000Z"))
	response, err := c.HTTP.Do(request)
	if err != nil {
		return SentAttachment{}, redactError(err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return SentAttachment{}, fmt.Errorf("couldn't upload the file to OLX: %w", readError(response))
	}
	var uploaded struct {
		Data struct {
			Filename string `json:"filename"`
		} `json:"data"`
	}
	if err = json.NewDecoder(response.Body).Decode(&uploaded); err != nil || uploaded.Data.Filename == "" {
		return SentAttachment{}, errors.New("couldn't read OLX's answer to an upload")
	}
	// What the upload service calls the file's name is its ID in a message.
	return SentAttachment{Filename: filename, FileID: uploaded.Data.Filename}, nil
}
