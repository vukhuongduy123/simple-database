package btree

import (
	"os"
)

const pageSize = 4096 // Page size

// Pager manages pages in a file
type Pager struct {
	file  *os.File // file to store pages
	count int64    // count the number of writing pages
}

// OpenPager opens a file for page management
func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	count := stat.Size() / (pageSize)

	p := &Pager{file: file, count: count}

	return p, nil
}

// WriteTo writes data to a specific page
func (p *Pager) WriteTo(pageID int64, data []byte) error {
	// if data is less than PAGE_SIZE, we need to pad it with null bytes
	if len(data) < pageSize {
		data = append(data, make([]byte, pageSize-len(data))...)
	}

	// write the data to the file
	_, err := p.file.WriteAt(data, pageSize*pageID)
	if err != nil {
		return err
	}

	return nil
}

func (p *Pager) NextPageId() (int64, error) {
	fileInfo, err := p.file.Stat()
	if err != nil {
		return -1, err
	}
	return fileInfo.Size() / pageSize, nil
}

// Close closes the file
func (p *Pager) Close() error {
	// sync one last time
	if err := p.file.Sync(); err != nil {
		return err
	}

	return p.file.Close()
}

// GetPage gets a page and returns the data
// Will gather all the pages that are linked together
func (p *Pager) GetPage(pageID int64) ([]byte, error) {
	// get the page
	data := make([]byte, pageSize)

	_, err := p.file.ReadAt(data, pageID*pageSize)

	if err != nil {
		return nil, err
	}

	return data, nil
}

// Count returns the number of pages
func (p *Pager) Count() int64 {
	return p.count
}
