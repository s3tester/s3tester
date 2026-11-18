package main

import (
	"fmt"
	"strconv"
	"sync/atomic"
)

const (
	keyAssignmentStatic  = "static"
	keyAssignmentDynamic = "dynamic"

	keyOrderSequential = "sequential"
	keyOrderRandom     = "random"
)

var (
	keyAssignments = []string{keyAssignmentStatic, keyAssignmentDynamic}
	keyOrders      = []string{keyOrderSequential, keyOrderRandom}
)

// keyCursor hands out key indexes to workers as they become free. A single cursor is shared by
// every worker on every endpoint of a test, so workers that hit slow objects claim fewer keys and
// all workers run out of keys at the same time.
type keyCursor struct {
	next atomic.Uint64
	// total is the number of distinct keys, or 0 for an unbounded key space.
	total uint64
	// wrap restarts from the first key once every key has been claimed instead of stopping.
	wrap bool
	perm *keyPermutation
}

// newKeyCursor returns the shared cursor for args, or nil when keys are statically assigned.
func newKeyCursor(args *Parameters) *keyCursor {
	if args.KeyAssignment != keyAssignmentDynamic {
		return nil
	}
	c := &keyCursor{}
	if args.IsDurationOperation() {
		return c
	}
	c.total = uint64(args.Requests)
	c.wrap = args.Duration > 0
	if args.KeyOrder == keyOrderRandom {
		c.perm = newKeyPermutation(c.total, uint64(args.KeySeed))
	}
	return c
}

// claim returns the next key index, or false once every key has been claimed.
func (c *keyCursor) claim() (uint64, bool) {
	i := c.next.Add(1) - 1
	if c.total == 0 {
		return i, true
	}
	if i >= c.total {
		if !c.wrap {
			return 0, false
		}
		i %= c.total
	}
	if c.perm != nil {
		i = c.perm.at(i)
	}
	return i, true
}

func formatKeyName(prefix string, index uint64, incrementing bool, formatString string) string {
	if incrementing {
		return prefix + "-" + fmt.Sprintf(formatString, index)
	}
	return prefix + "-" + strconv.FormatUint(index, 10)
}

// keyPermutation is a keyed bijection on [0, n) that needs no per-key memory. It is a 4-round
// Feistel network over the smallest even power of two covering n, using cycle-walking to map
// values that fall outside [0, n) back inside it.
type keyPermutation struct {
	n        uint64
	halfBits uint
	halfMask uint64
	keys     [4]uint64
}

func newKeyPermutation(n, seed uint64) *keyPermutation {
	bits := uint(1)
	for (uint64(1) << bits) < n {
		bits++
	}
	if bits%2 == 1 {
		bits++
	}
	p := keyPermutation{n: n, halfBits: bits / 2, halfMask: (uint64(1) << (bits / 2)) - 1}
	for i := range p.keys {
		seed = mix64(seed + uint64(i) + 1)
		p.keys[i] = seed
	}
	return &p
}

// mix64 is the SplitMix64 finalizer.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func (p *keyPermutation) at(i uint64) uint64 {
	x := i
	for {
		l, r := x>>p.halfBits, x&p.halfMask
		for _, k := range p.keys {
			l, r = r, l^(mix64(r^k)&p.halfMask)
		}
		x = l<<p.halfBits | r
		if x < p.n {
			return x
		}
	}
}
