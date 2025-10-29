package index

type Page struct {
	StartPos int64
}

func NewPage(startPos int64) *Page {
	return &Page{StartPos: startPos}
}
