package dirtyset

import (
	"fmt"
	"testing"
)

func BenchmarkCollapse100K(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set := New("/data")
		for n := 0; n < 100_000; n++ {
			set.Add(fmt.Sprintf("/data/%02x/%02x/object-%d", n%256, (n/256)%256, n))
		}
		if set.Len() == 0 {
			b.Fatal("empty dirty set")
		}
	}
	b.ReportMetric(100_000*float64(b.N)/b.Elapsed().Seconds(), "paths/s")
}
