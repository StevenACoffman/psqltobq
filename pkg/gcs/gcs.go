package gcs

import (
	"context"
	"fmt"
	"github.com/StevenACoffman/psqltobq/pkg/ds"
	"io"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"

	"github.com/StevenACoffman/anotherr/errors"
)

// UploadFile uploads an object given the full src file path
func UploadFile(ctx context.Context, gcsClient *storage.Client, info *ds.ImportTableInfo) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*180)
	defer cancel()

	info.ObjectName = fmt.Sprintf(
		"%s/%s/%s",
		info.GCSFolder,
		info.LastUpdatedTSStr,
		info.LocalFileName,
	)
	o := gcsClient.Bucket(info.GCSBucket).Object(info.ObjectName)
	sourceFileStat, err := os.Stat(info.LocalFileName)
	if err != nil {
		return errors.Wrap(err, "Unable to stat file "+info.LocalFileName)
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", info.LocalFileName)
	}
	source, err := os.Open(info.LocalFileName)
	if err != nil {
		return errors.Wrap(err, "Unable to open file "+info.LocalFileName)
	}
	defer func(source *os.File) {
		_ = source.Close()
	}(source)

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)
	if _, err = io.Copy(wc, source); err != nil {
		return errors.Wrap(err, "Unable to io.Copy file "+info.LocalFileName)
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
