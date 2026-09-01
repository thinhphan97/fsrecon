//go:build windows

package scanner

import (
	"fmt"
	"io/fs"
	"syscall"
)

func fileIdentity(path string, _ fs.FileInfo) (string, uint64, error) {
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return "", 0, err
	}
	handle, err := syscall.CreateFile(
		ptr,
		0x80, // FILE_READ_ATTRIBUTES
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		0x02000000|0x00200000, // BACKUP_SEMANTICS | OPEN_REPARSE_POINT
		0,
	)
	if err != nil {
		return "", 0, err
	}
	defer syscall.CloseHandle(handle)
	var data syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(handle, &data); err != nil {
		return "", 0, err
	}
	return fmt.Sprintf("%x:%x%x", data.VolumeSerialNumber, data.FileIndexHigh, data.FileIndexLow), uint64(data.NumberOfLinks), nil
}
