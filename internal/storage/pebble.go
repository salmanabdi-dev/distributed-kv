package storage

import (
	"github.com/cockroachdb/pebble"
)

// PebbleEngine adapts a *pebble.DB to the Engine interface. Pebble is an
// embedded, pure-Go LSM-tree engine (no cgo), API- and design-compatible
// with LevelDB/RocksDB, actively maintained by CockroachDB. It was chosen
// over cgo-based LevelDB bindings (fragile cross-compilation, especially in
// slim Docker images) and over goleveldb (effectively unmaintained).
type PebbleEngine struct {
	db *pebble.DB
}

// OpenPebble opens (creating if necessary) a Pebble instance at dir.
func OpenPebble(dir string) (*PebbleEngine, error) {
	opts := &pebble.Options{}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleEngine{db: db}, nil
}

func (p *PebbleEngine) Get(key []byte) ([]byte, error) {
	v, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	// Copy: v is only valid until closer.Close().
	out := make([]byte, len(v))
	copy(out, v)
	if cerr := closer.Close(); cerr != nil {
		return nil, cerr
	}
	return out, nil
}

func (p *PebbleEngine) Set(key, value []byte, opts WriteOptions) error {
	return p.db.Set(key, value, syncOpt(opts))
}

func (p *PebbleEngine) Delete(key []byte, opts WriteOptions) error {
	return p.db.Delete(key, syncOpt(opts))
}

func (p *PebbleEngine) NewBatch() Batch {
	return &pebbleBatch{batch: p.db.NewBatch()}
}

func (p *PebbleEngine) NewIterator(lowerBound, upperBound []byte) (Iterator, error) {
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	it.First()
	return &pebbleIterator{it: it}, nil
}

func (p *PebbleEngine) Close() error {
	return p.db.Close()
}

func syncOpt(opts WriteOptions) *pebble.WriteOptions {
	if opts.Sync {
		return pebble.Sync
	}
	return pebble.NoSync
}

type pebbleBatch struct {
	batch *pebble.Batch
}

func (b *pebbleBatch) Set(key, value []byte) { _ = b.batch.Set(key, value, nil) }
func (b *pebbleBatch) Delete(key []byte)     { _ = b.batch.Delete(key, nil) }

func (b *pebbleBatch) Commit(opts WriteOptions) error {
	return b.batch.Commit(syncOpt(opts))
}

type pebbleIterator struct {
	it *pebble.Iterator
}

func (i *pebbleIterator) Valid() bool   { return i.it.Valid() }
func (i *pebbleIterator) Next()         { i.it.Next() }
func (i *pebbleIterator) Key() []byte   { return append([]byte(nil), i.it.Key()...) }
func (i *pebbleIterator) Value() []byte { return append([]byte(nil), i.it.Value()...) }
func (i *pebbleIterator) Close() error  { return i.it.Close() }
