package btree

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const pageSize = 1024 // Page size

// Pager manages pages in a file
type Pager struct {
	file             *os.File      // file to store pages
	deletedPages     []int64       // list of deleted pages
	deletedPagesLock *sync.Mutex   // lock for deletedPages
	deletedPagesFile *os.File      // file to store deleted pages
	count            int64         // cached count of pages
	syncInterval     time.Duration // interval to sync the file
	exit             chan struct{} // exit channel
	wg               *sync.WaitGroup
}

// OpenPager opens a file for page management
func OpenPager(filename string, syncInterval time.Duration) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	// open the deleted pages file
	deletedPagesFile, err := os.OpenFile(filename+".del", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	// read the deleted pages
	deletedPages, err := readDelPages(deletedPagesFile)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	count := stat.Size() / (pageSize)

	p := &Pager{file: file,
		deletedPages:     deletedPages,
		deletedPagesFile: deletedPagesFile,
		deletedPagesLock: &sync.Mutex{},
		exit:             make(chan struct{}),
		count:            count,
		syncInterval:     syncInterval,
		wg:               &sync.WaitGroup{}}

	p.wg.Add(1)
	go p.sync()

	return p, nil
}

func (p *Pager) sync() {
	ticker := time.NewTicker(p.syncInterval)
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

// writeDelPages writes the deleted pages that are in-memory to the deleted pages file
func (p *Pager) writeDelPages() error {

	// Truncate the file
	err := p.deletedPagesFile.Truncate(0)
	if err != nil {
		return err
	}

	// Seek to the start of the file
	_, err = p.deletedPagesFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Write the deleted pages to the file
	_, err = p.deletedPagesFile.WriteAt([]byte(strings.Join(strings.Fields(fmt.Sprint(p.deletedPages)), ",")), 0)
	if err != nil {
		return err
	}

	return nil
}

// readDelPages reads the deleted pages from the deleted pages file
func readDelPages(file *os.File) ([]int64, error) {
	pages := make([]int64, 0)

	// stored in comma-separated format
	// i.e., 1,2,3,4,5
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return pages, nil
	}

	data = bytes.TrimLeft(data, "[")
	data = bytes.TrimRight(data, "]")

	// split the data into pages
	pagesStr := strings.Split(string(data), ",")

	for _, pageStr := range pagesStr {
		// convert the string to int64
		page, err := strconv.ParseInt(pageStr, 10, 64)
		if err != nil {
			continue
		}

		pages = append(pages, page)

	}

	return pages, nil
}

// WriteTo writes data to a specific page
func (p *Pager) WriteTo(pageID int64, data []byte) error {
	err := p.DeletePage(pageID)
	if err != nil {
		return err
	}
	// remove from deleted pages
	p.deletedPagesLock.Lock()
	defer p.deletedPagesLock.Unlock()

	for i, page := range p.deletedPages {
		if page == pageID {
			p.deletedPages = append(p.deletedPages[:i], p.deletedPages[i+1:]...)
		}
	}

	// if data is less than PAGE_SIZE, we need to pad it with null bytes
	if len(data) < pageSize {
		data = append(data, make([]byte, pageSize-len(data))...)
	}

	// write the data to the file
	_, err = p.file.WriteAt(data, (pageSize)*pageID)
	if err != nil {
		return err
	}

	return nil
}

// Write writes data to the next available page
func (p *Pager) Write(data []byte) (int64, error) {
	// check if there are any deleted pages
	if len(p.deletedPages) > 0 {
		// get the last deleted page
		pageID := p.deletedPages[len(p.deletedPages)-1]
		p.deletedPages = p.deletedPages[:len(p.deletedPages)-1]

		err := p.WriteTo(pageID, data)
		if err != nil {
			return -1, err
		}

		return pageID, nil

	}
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

	// write the deleted pages to the file
	if err := p.writeDelPages(); err != nil {
		return err
	}
	return p.file.Close()
}

// GetPage gets a page and returns the data
// Will gather all the pages that are linked together
func (p *Pager) GetPage(pageID int64) ([]byte, error) {
	p.deletedPagesLock.Lock()
	// Check if in deleted pages, if so, return nil
	if slices.Contains(p.deletedPages, pageID) {
		p.deletedPagesLock.Unlock()
		return nil, nil
	}
	p.deletedPagesLock.Unlock()

	// get the page
	data := make([]byte, pageSize)

	_, err := p.file.ReadAt(data, pageID*(pageSize))

	if err != nil {
		return nil, err
	}

	return data, nil
}

// GetDeletedPages returns the list of deleted pages
func (p *Pager) GetDeletedPages() []int64 {
	p.deletedPagesLock.Lock()
	defer p.deletedPagesLock.Unlock()
	return p.deletedPages
}

// DeletePage deletes a page
func (p *Pager) DeletePage(pageID int64) error {
	p.deletedPagesLock.Lock()
	defer p.deletedPagesLock.Unlock()

	// Add the page to the deleted pages
	p.deletedPages = append(p.deletedPages, pageID)

	// write the deleted pages to the file
	err := p.writeDelPages()
	if err != nil {
		return err
	}

	return nil
}

// Count returns the number of pages
func (p *Pager) Count() int64 {
	return p.count
}
