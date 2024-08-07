package gcpapi

import (
	"context"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/StevenACoffman/anotherr/errors"
)

func NewCloudStorageClient(
	ctx context.Context,
	credentials []byte,
) (*storage.Client, error) {
	var gcsClient *storage.Client
	var cErr error
	if len(credentials) > 0 {
		gcsClient, cErr = storage.NewClient(
			ctx,
			option.WithCredentialsJSON(credentials),
		)
	} else {
		gcsClient, cErr = storage.NewClient(ctx)
	}
	return gcsClient, errors.Wrap(cErr, "Unable to get New Cloud Storage client")
}
