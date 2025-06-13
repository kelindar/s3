// Copyright 2023 Sneller, Inc.
// Copyright 2025 Roman Atachiants
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package fsutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"slices"

	"github.com/stretchr/testify/assert"
)

func TestVisitDir(t *testing.T) {
	// must be sorted
	list := []string{
		"a.txt",
		"b.txt",
		"c.txt",
		"foo",
		"z.txt",
	}
	tmp := t.TempDir()
	for i := range list {
		err := os.WriteFile(filepath.Join(tmp, list[i]), []byte{}, 0640)
		assert.NoError(t, err, "creating file %q", list[i])
	}
	cases := []struct {
		seek, pattern string
	}{
		{"", ""},
		{"c.txt", ""},
		{"", "*.txt"},
		{"", "foo"},
		{"foo", "*.txt"},
	}
	// trivial implementation
	trivial := func(seek, pattern string) []string {
		var out []string
		for i := range list {
			if list[i] <= seek {
				continue
			}
			if pattern != "" {
				m, err := path.Match(pattern, list[i])
				assert.NoError(t, err)
				if !m {
					continue
				}
			}
			out = append(out, list[i])
		}
		return out
	}
	dir := os.DirFS(tmp)
	for i := range cases {
		seek := cases[i].seek
		pattern := cases[i].pattern
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			var got []string
			err := VisitDir(dir, ".", seek, pattern, func(d DirEntry) error {
				got = append(got, d.Name())
				return nil
			})
			assert.NoError(t, err)
			want := trivial(seek, pattern)
			assert.Equal(t, want, got, "walk(%q, %q) mismatch", seek, pattern)
		})
	}
}

// trivialWalkDir trivially implements the
// behavior of WalkDir without the added
// benefits of plumbing seek and pattern down to
// the directory listing code.
//
// This is the behavior we want to ensure that
// WalkDir correctly implements.
func trivialWalkDir(f fs.FS, name, seek, pattern string, fn WalkDirFn) error {
	return fs.WalkDir(f, name, func(p string, d fs.DirEntry, err error) error {
		if pattern != "" {
			match, err := path.Match(pattern, p)
			if err != nil || !match {
				return err
			}
		}
		if pathcmp(p, seek) > 0 {
			return fn(p, d, err)
		}
		return err
	})
}

// walkDirFn is any function with a signature
// like WalkDir.
type walkDirFn func(f fs.FS, name, seek, pattern string, fn WalkDirFn) error

// flatwalk returns all walked paths in a list.
func flatwalk(walkdir walkDirFn, f fs.FS, name, seek, pattern string, limit uint) ([]string, error) {
	var out []string
	err := walkdir(f, name, seek, pattern, func(p string, d DirEntry, err error) error {
		if limit > 0 && uint(len(out)) >= limit {
			panic("fs.SkipAll did not work as expected")
		}
		if err != nil {
			return err
		}
		out = append(out, p)
		if limit > 0 && uint(len(out)) >= limit {
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func FuzzWalkDir(f *testing.F) {
	// file tree to create
	list := []string{
		"a",
		"a/b",
		"a/b/c",
		"a/b/d.txt",
		"b",
		"b/c",
		"b/d",
		"b/d/e",
		"b/e",
		"b/e/f",
		"b/e/f/g",
		"b/e/f/g/h.txt",
		"b/e/g",
		"b/f.txt",
		"c",
		"c/d",
		"c/d/e",
		"c/e",
		"c/f.txt",
		"d",
		"d.txt",
	}
	// seeks to test
	seeks := append(list, []string{
		"",
		"*",
		".",
		"a/z",
		"b/c/d/e/f",
		"blah",
		"e.txt",
		"foo/bar",
		"z",
	}...)
	// patterns to test
	patterns := []string{
		"",
		"*",
		"*/*",
		"a/*",
		"*/[ac]",
		"b/e/f/g/*.txt",
	}
	// create test files
	tmp := f.TempDir()
	for i := range list {
		if !strings.Contains(list[i], ".") {
			err := os.Mkdir(filepath.Join(tmp, list[i]), 0750)
			assert.NoError(f, err, "creating dir %q", list[i])
		} else {
			err := os.WriteFile(filepath.Join(tmp, list[i]), []byte{}, 0640)
			assert.NoError(f, err, "creating file %q", list[i])
		}
	}
	dir := os.DirFS(tmp)
	for _, seek := range seeks {
		for _, pattern := range patterns {
			f.Add(seek, pattern, uint(0))
			f.Add(seek, pattern, uint(10))
		}
	}
	validate := func(seek, pattern string) bool {
		if seek != "" {
			if !fs.ValidPath(seek) {
				return false
			}
			// make sure path is not rejected by the
			// file system
			f, err := dir.Open(seek)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return false
			} else if err == nil {
				f.Close()
			}
		}
		if pattern != "" && !fs.ValidPath(pattern) {
			return false
		}
		// ignore patterns like "f[o/]o" for now...
		for n := 1; ; n++ {
			p, rest, ok := trim(pattern, n)
			if !ok {
				return false
			}
			if _, err := path.Match(p, ""); err != nil {
				return false
			}
			if rest == "" {
				break
			}
		}
		return true
	}
	f.Fuzz(func(t *testing.T, seek, pattern string, limit uint) {
		// ignore invalid arguments
		if !validate(seek, pattern) {
			t.Skipf("skipping invalid arguments seek=%q pattern=%q", seek, pattern)
		}
		for i, name := range list {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				got, err := flatwalk(WalkDir, dir, name, seek, pattern, limit)
				assert.NoError(t, err, "WalkDir(%q, %q, %q, %q) returned error", dir, name, seek, pattern)
				want, err := flatwalk(trivialWalkDir, dir, name, seek, pattern, limit)
				assert.NoError(t, err, "trivialWalkDir(%q, %q, %q, %q) returned error", dir, name, seek, pattern)
				assert.Equal(t, want, got, "walk(%q, %q, %q, %d) mismatch", name, seek, pattern, limit)
			})
		}
	})
}

func FuzzSegments(f *testing.F) {
	trivial := func(p string) (int, bool) {
		if p == "" || p == "." {
			return 0, true
		}
		if !fs.ValidPath(p) {
			return 0, false
		}
		return strings.Count(p, "/") + 1, true
	}
	for _, s := range []string{
		"",
		".",
		"..",
		"/",
		"a",
		"a/b",
		"a/b/c",
		"foo",
		"foo/bar",
		"foo/bar/baz",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, p string) {
		got, gotok := segments(p)
		want, wantok := trivial(p)
		assert.Equal(t, want, got, "segments(%q): want %d, got %d", p, want, got)
		assert.Equal(t, wantok, gotok, "segments(%q): want ok=%v, got ok=%v", p, wantok, gotok)
	})
}

func FuzzTrim(f *testing.F) {
	trivial := func(p string, n int) (front, next string, ok bool) {
		if p != "" && !fs.ValidPath(p) {
			return "", "", false
		}
		if p == "" || p == "." {
			if n == 0 {
				return "", p, true
			}
			return p, "", true
		}
		join := func(ps []string) string {
			return path.Join(ps...)
		}
		ps := strings.Split(p, "/")
		if len(ps) <= n {
			return join(ps), "", true
		}
		return join(ps[:n]), ps[n], true
	}
	paths := []string{
		"",
		"*",
		".",
		"a",
		"a/*",
		"a/*/c",
		"a/?/*.txt",
		"a/b",
		"a/b/c",
		"foo/bar/baz/quux",
	}
	for _, p := range paths {
		for n := uint(0); n < 10; n++ {
			f.Add(p, n)
		}
	}
	f.Fuzz(func(t *testing.T, p string, n uint) {
		f0, n0, ok0 := trivial(p, int(n))
		f1, n1, ok1 := trim(p, int(n))
		assert.Equal(t, f0, f1, "trim(%q, %d): want front=%q, got front=%q", p, n, f0, f1)
		assert.Equal(t, n0, n1, "trim(%q, %d): want next=%q, got next=%q", p, n, n0, n1)
		assert.Equal(t, ok0, ok1, "trim(%q, %d): want ok=%v, got ok=%v", p, n, ok0, ok1)
	})
}

func FuzzPathcmp(f *testing.F) {
	trivial := func(a, b string) int {
		if a == "." {
			a = ""
		}
		if b == "." {
			b = ""
		}
		as := strings.Split(a, "/")
		bs := strings.Split(b, "/")
		return slices.Compare(as, bs)
	}
	cases := []struct {
		a, b string
	}{
		{"", ""},
		{"", "."},
		{".", "."},
		{".", "a"},
		{"a", "."},
		{"a", "a/b"},
		{"a/b", "."},
		{"a/b/c", "a/b"},
		{"foo/bar", "a/b"},
	}
	for i := range cases {
		f.Add(cases[i].a, cases[i].b)
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		test := func(a, b string) {
			t.Helper()
			got := pathcmp(a, b)
			want := trivial(a, b)
			assert.Equal(t, want, got, "pathcmp(%q, %q): want %d, got %d", a, b, want, got)
		}
		test(a, b)
		test(b, a)
	})
}

func FuzzTreecmp(f *testing.F) {
	trivial := func(root, p string) int {
		if root == "." {
			return 0
		}
		if p == "." {
			return -1
		}
		if root == p || strings.HasPrefix(p, root) && p[len(root)] == '/' {
			return 0
		}
		// make a file tree
		tree := []string{
			root,
			path.Join(root, "foo"),
			path.Join(root, "foo/bar"),
		}
		// insert p
		tree = append(tree, p)
		// sort it lexically
		slices.SortFunc(tree, func(a, b string) int {
			return pathcmp(a, b)
		})
		// look for p
		if tree[0] == p {
			return -1
		}
		if tree[len(tree)-1] == p {
			return 1
		}
		return 0
	}
	cases := []struct {
		a, b string
	}{
		{".", "."},
		{".", "a"},
		{"a", "a/b"},
		{"a/b", "."},
		{"a/b/c", "a/b"},
		{"foo/bar", "a/b"},
		{"c/e", "c/d/e"},
	}
	for i := range cases {
		f.Add(cases[i].a, cases[i].b)
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		test := func(a, b string) {
			if !fs.ValidPath(a) || !fs.ValidPath(b) {
				return
			}
			t.Helper()
			got := treecmp(a, b)
			want := trivial(a, b)
			assert.Equal(t, want, got, "treecmp(%q, %q): want %d, got %d", a, b, want, got)
		}
		test(a, b)
		test(b, a)
	})
}
