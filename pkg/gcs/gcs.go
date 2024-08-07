package gcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"go.uber.org/zap"

	"github.com/StevenACoffman/anotherr/errors"
	"github.com/StevenACoffman/psqltobq/pkg/ds"
)

// UploadFile uploads an object given the full src file path
func UploadFile(ctx context.Context, logger *zap.Logger, gcsClient *storage.Client, info *ds.ImportTableInfo) error {
	info.ObjectName = fmt.Sprintf(
		"%s/%s/%s",
		info.GCSFolder,
		info.LastUpdatedTSStr,
		info.LocalFileName,
	)
	logger.Info("Uploading to GCS " + info.GCSBucket + "/" + info.ObjectName)

	o := gcsClient.Bucket(info.GCSBucket).Object(info.ObjectName)
	sourceFileStat, err := os.Stat(info.LocalFullFilePath)
	if err != nil {
		return errors.Wrap(err, "Unable to stat file "+info.LocalFullFilePath)
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", info.LocalFileName)
	}
	defer func() {
		_ = os.Remove(info.LocalFullFilePath)
	}()
	source, err := os.Open(info.LocalFullFilePath)
	if err != nil {
		return errors.Wrap(err, "Unable to open file "+info.LocalFullFilePath)
	}
	defer func(source *os.File) {
		_ = source.Close()
	}(source)

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)
	if _, err = io.Copy(wc, source); err != nil {
		return errors.Wrap(err, "Unable to io.Copy file "+info.LocalFullFilePath)
	}

	if err = wc.Close(); err != nil {
		return errors.Wrapf(
			err,
			"Unable to Close storage Writer for objectName %v",
			info.ObjectName,
		)
	}

	_, err = o.Update(ctx, storage.ObjectAttrsToUpdate{
		ContentType:        "text/csv; charset=utf-8",
		ContentDisposition: "attachment;filename=" + filepath.Base(info.ObjectName),
		// we need to preserve the modTime as a CustomTime attribute to enable the DataTeam
		// KhanFlow pipeline to determine if the files have changed.
		CustomTime: info.LastUpdated,
	})
	if err != nil {
		return errors.Wrapf(
			err,
			"Unable to Update ObjectAttrsToUpdate for objectName %s",
			info.ObjectName,
		)
	}

	return nil
}
