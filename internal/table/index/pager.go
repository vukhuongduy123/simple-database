package index

import (
	"io"
	"os"
	platformerror "simple-database/internal/platform/error"
)

type Pager struct {
	f        *os.File
	pageSize int64
}

func pageOffset(pageNumber int64, pageSize int64) int64 {
	return pageNumber * pageSize
}

func NewPager(f *os.File, pageSize int64) *Pager {
	return &Pager{f: f, pageSize: pageSize}
}

func (p *Pager) Close() error {
	return p.f.Close()
}

func (p *Pager) write(node *node) error {
	if _, err := p.f.Seek(pageOffset(node.pageNumber, p.pageSize), io.SeekStart); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCode)
	}

	buf, err := node.MarshalBinary()
	if err != nil {
		return err
	}

	if _, err = p.f.Write(buf); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}

	if err = p.f.Sync(); err != nil {
		return platformerror.NewStackTraceError(err.Error(), platformerror.BinaryWriteErrorCode)
	}
	return nil
}

func (p *Pager) read(pageNumber int64) (*node, error) {
	offset := pageOffset(pageNumber, p.pageSize)
	if _, err := p.f.Seek(offset, io.SeekStart); err != nil {
		return nil, platformerror.NewStackTraceError(err.Error(), platformerror.FileSeekErrorCode)
	}

	data := make([]byte, p.pageSize)
	_, err := p.f.Read(data[:])
	if err != nil {
		return nil, err
	}
	node := &node{}
	if err = node.UnmarshalBinary(data); err != nil {
		return nil, err
	}

	return node, nil
}
