package structlog

// rowBatch hands rows on in groups of size so a transaction never holds all of its rows at once.
type rowBatch struct {
	rows    []Structlog
	size    int
	flushFn func([]Structlog) error
}

func newRowBatch(size int, flushFn func([]Structlog) error) *rowBatch {
	if size <= 0 {
		size = DefaultBufferMaxRows
	}

	return &rowBatch{rows: make([]Structlog, 0, min(size, 4096)), size: size, flushFn: flushFn}
}

func (b *rowBatch) add(row *Structlog) error {
	b.rows = append(b.rows, *row)

	if len(b.rows) >= b.size {
		return b.flush()
	}

	return nil
}

func (b *rowBatch) flush() error {
	if len(b.rows) == 0 {
		return nil
	}

	err := b.flushFn(b.rows)
	b.rows = b.rows[:0]

	return err
}
