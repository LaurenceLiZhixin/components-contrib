package dubbo

import (
	"bytes"
	"encoding/binary"

	perrors "github.com/pkg/errors"
)

// ReadInt32 reads an int32 in big endian from the buffer
func ReadInt32(buf *bytes.Buffer) (int32, error) {
	var b = make([]byte, 4)
	if _, err := buf.Read(b); err != nil {
		return 0, perrors.WithStack(err)
	} else {
		return int32(binary.BigEndian.Uint32(b)), nil
	}
}
