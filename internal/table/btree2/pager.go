package btree2

import (
	"os"
	"sync"
	"time"
)

const pageSize = 1024 // Page size

// Pager manages pages in a file
type Pager struct {
	file         *os.File      // file to store pages
	count        int64         // cached count of pages
	syncInterval time.Duration // interval to sync the file
	exit         chan struct{} // exit channel
	wg           *sync.WaitGroup
}

// OpenPager opens a file for page management
func OpenPager(filename string, syncInterval time.Duration) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	count := stat.Size() / (pageSize)

	p := &Pager{file: file,
		exit:         make(chan struct{}),
		count:        count,
		syncInterval: syncInterval,
		wg:           &sync.WaitGroup{}}

	p.wg.Add(1)
	go p.sync()

	return p, nil
}

func (p *Pager) sync() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := p.file.Sync()
			if err != nil {
				panic(err)
			}
		case <-p.exit:
			ticker.Stop()
			return
		}
	}
}

// WriteTo writes data to a specific page
func (p *Pager) WriteTo(pageID int64, data []byte) error {
	// if data is less than PAGE_SIZE, we need to pad it with null bytes
	if len(data) < pageSize {
		data = append(data, make([]byte, pageSize-len(data))...)
	}

	// write the data to the file
	_, err := p.file.WriteAt(data, (pageSize)*pageID)
	if err != nil {
		return err
	}

	return nil
}

// Write writes data to the next available page
func (p *Pager) Write(data []byte) (int64, error) {
	// get the current file size
	fileInfo, err := p.file.Stat()
	if err != nil {
		return -1, err
	}

	if fileInfo.Size() == 0 {
		err = p.WriteTo(0, data)
		if err != nil {
			return -1, err
		}
		p.count++
		return 0, nil
	}

	// create a new page
	pageId := fileInfo.Size() / (pageSize)

	err = p.WriteTo(pageId, data)
	if err != nil {
		return -1, err
	}
	p.count++
	return pageId, nil
}

// Close closes the file
func (p *Pager) Close() error {
	// close the exit channel
	close(p.exit)
	p.wg.Wait() // wait for the sync goroutine to finish

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

	_, err := p.file.ReadAt(data, pageID*(pageSize))

	if err != nil {
		return nil, err
	}

	return data, nil
}

// Count returns the number of pages
func (p *Pager) Count() int64 {
	return p.count
}
