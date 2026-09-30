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

package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/kelindar/s3/aws"
	"github.com/kelindar/s3/fsutil"
	"github.com/kelindar/s3/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefix(t *testing.T) {
	t.Run("basic properties", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Create test structure
		mockServer.PutObject("dir1/file1.txt", []byte("content1"))
		mockServer.PutObject("dir1/file2.txt", []byte("content2"))
		mockServer.PutObject("dir1/subdir/file3.txt", []byte("content3"))

		b := NewBucket(key, bucket)

		// Open directory
		dir, err := b.Open("dir1")
		assert.NoError(t, err)
		defer dir.Close()

		prefix, ok := dir.(*Prefix)
		assert.True(t, ok)

		// Test basic properties
		assert.Equal(t, "dir1", prefix.Name())
		assert.True(t, prefix.IsDir())
		assert.Equal(t, fs.ModeDir|0755, prefix.Mode())
		assert.Equal(t, fs.ModeDir, prefix.Type())
		assert.Equal(t, int64(0), prefix.Size())
		assert.Equal(t, time.Time{}, prefix.ModTime()) // Prefixes don't have mod times
		assert.Nil(t, prefix.Sys())

		// Test Stat
		info, err := prefix.Stat()
		assert.NoError(t, err)
		assert.Equal(t, prefix, info)

		// Test Info (fs.DirEntry interface)
		dirInfo, err := prefix.Info()
		assert.NoError(t, err)
		assert.Equal(t, info, dirInfo)
	})

	t.Run("read dir", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Create test structure
		mockServer.PutObject("testdir/file1.txt", []byte("content1"))
		mockServer.PutObject("testdir/file2.txt", []byte("content2"))
		mockServer.PutObject("testdir/subdir/file3.txt", []byte("content3"))
		mockServer.PutObject("testdir/another/file4.txt", []byte("content4"))

		b := NewBucket(key, bucket)

		// Open directory
		dir, err := b.Open("testdir")
		assert.NoError(t, err)
		defer dir.Close()

		prefix, ok := dir.(*Prefix)
		assert.True(t, ok)

		// Test ReadDir(-1) - read all entries
		entries, err := prefix.ReadDir(-1)
		assert.NoError(t, err)
		assert.Len(t, entries, 4) // 2 files + 2 subdirs

		// Verify entries are sorted
		names := make([]string, len(entries))
		for i, entry := range entries {
			names[i] = entry.Name()
		}
		assert.Equal(t, []string{"another", "file1.txt", "file2.txt", "subdir"}, names)

		// Test ReadDir(2) - read limited entries
		prefix.token = ""
		prefix.dirEOF = false
		entries, err = prefix.ReadDir(2)
		assert.NoError(t, err)
		assert.Len(t, entries, 2)
		assert.Equal(t, "another", entries[0].Name())
		assert.Equal(t, "file1.txt", entries[1].Name())

		// Test ReadDir after EOF
		prefix.dirEOF = true
		entries, err = prefix.ReadDir(-1)
		assert.Empty(t, entries)
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("open", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Create test structure
		mockServer.PutObject("parent/child/file.txt", []byte("content"))
		mockServer.PutObject("parent/file2.txt", []byte("content2"))

		b := NewBucket(key, bucket)

		// Open parent directory
		parent, err := b.Open("parent")
		assert.NoError(t, err)
		defer parent.Close()

		prefix, ok := parent.(*Prefix)
		assert.True(t, ok)

		// Test opening current directory
		current, err := prefix.Open(".")
		assert.NoError(t, err)
		assert.Equal(t, prefix, current)

		// Test opening subdirectory
		child, err := prefix.Open("child")
		assert.NoError(t, err)
		defer child.Close()

		childPrefix, ok := child.(*Prefix)
		assert.True(t, ok)
		assert.Equal(t, "child", childPrefix.Name())

		// Test opening non-existent directory
		_, err = prefix.Open("nonexistent")
		assert.Error(t, err)
		assert.ErrorIs(t, err, fs.ErrNotExist)

		// Test opening with invalid path
		_, err = prefix.Open("../invalid")
		assert.Error(t, err)
	})

	t.Run("read", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		mockServer.PutObject("dir/file.txt", []byte("content"))

		b := NewBucket(key, bucket)

		dir, err := b.Open("dir")
		assert.NoError(t, err)
		defer dir.Close()

		prefix, ok := dir.(*Prefix)
		assert.True(t, ok)

		// Reading from a directory should always return an error
		buf := make([]byte, 10)
		n, err := prefix.Read(buf)
		assert.Equal(t, 0, n)
		assert.Error(t, err)
		assert.ErrorIs(t, err, fs.ErrInvalid)
	})

	t.Run("close", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		mockServer.PutObject("dir/file.txt", []byte("content"))

		b := NewBucket(key, bucket)

		dir, err := b.Open("dir")
		assert.NoError(t, err)

		prefix, ok := dir.(*Prefix)
		assert.True(t, ok)

		// Close should always succeed for directories
		err = prefix.Close()
		assert.NoError(t, err)
	})

	t.Run("visit dir", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Create test structure
		mockServer.PutObject("visit/a.txt", []byte("a"))
		mockServer.PutObject("visit/b.txt", []byte("b"))
		mockServer.PutObject("visit/c.txt", []byte("c"))
		mockServer.PutObject("visit/sub/d.txt", []byte("d"))

		prefix := &Prefix{
			Key:    key,
			Bucket: bucket,
			Path:   "visit/",
		}

		// Test VisitDir with pattern
		var visited []string
		walkFn := func(entry fsutil.DirEntry) error {
			visited = append(visited, entry.Name())
			return nil
		}

		err := prefix.VisitDir(".", "", "*.txt", walkFn)
		assert.NoError(t, err)
		assert.Contains(t, visited, "a.txt")
		assert.Contains(t, visited, "b.txt")
		assert.Contains(t, visited, "c.txt")

		// Test VisitDir with seek
		visited = nil
		err = prefix.VisitDir(".", "b.txt", "*.txt", walkFn)
		assert.NoError(t, err)
		// Should start after b.txt
		assert.Contains(t, visited, "c.txt")
		assert.NotContains(t, visited, "a.txt")
		assert.NotContains(t, visited, "b.txt")
	})

	t.Run("root directory", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Create files in root
		mockServer.PutObject("root1.txt", []byte("content1"))
		mockServer.PutObject("root2.txt", []byte("content2"))

		b := NewBucket(key, bucket)

		// Open root directory
		root, err := b.Open(".")
		assert.NoError(t, err)
		defer root.Close()

		prefix, ok := root.(*Prefix)
		assert.True(t, ok)
		assert.Equal(t, ".", prefix.Name())

		// Test reading root directory
		entries, err := prefix.ReadDir(-1)
		assert.NoError(t, err)
		assert.Len(t, entries, 2)
	})

	t.Run("empty directory", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		b := NewBucket(key, bucket)

		// Try to open non-existent directory
		_, err := b.Open("nonexistent")
		assert.Error(t, err)
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("sub prefix", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		prefix := &Prefix{
			Key:    key,
			Bucket: bucket,
			Path:   "parent/",
		}

		// Test sub method
		sub := prefix.sub("child")
		assert.Equal(t, "parent/child", sub.Path)
		assert.Equal(t, key, sub.Key)
		assert.Equal(t, bucket, sub.Bucket)

		// Test join method
		joined := prefix.join("extra")
		assert.Equal(t, "parent/extra", joined)

		// Test join with root prefix
		rootPrefix := &Prefix{Path: "."}
		rootJoined := rootPrefix.join("test")
		assert.Equal(t, "test", rootJoined)
	})

	t.Run("open dir errors", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Test openDir with invalid bucket
		invalidPrefix := &Prefix{
			Key:    key,
			Bucket: "invalid_bucket",
			Path:   "test/",
		}

		_, err := invalidPrefix.openDir()
		assert.Error(t, err)
	})

	t.Run("list errors", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Test list with invalid bucket
		invalidPrefix := &Prefix{
			Key:    key,
			Bucket: "invalid_bucket",
			Path:   "test/",
		}

		_, err := invalidPrefix.list(100, "", "", "")
		assert.Error(t, err)
	})

	t.Run("read dir at", func(t *testing.T) {
		bucket := "test-bucket"
		mockServer := mock.New(bucket, "us-east-1")
		defer mockServer.Close()

		key := aws.DeriveKey("", "fake-access-key", "fake-secret-key", "us-east-1", "s3")
		key.BaseURI = mockServer.URL()

		// Create test structure with many files
		for i := 0; i < 10; i++ {
			mockServer.PutObject(fmt.Sprintf("test/file%02d.txt", i), []byte(fmt.Sprintf("content%d", i)))
		}

		prefix := &Prefix{
			Key:    key,
			Bucket: bucket,
			Path:   "test/",
		}

		// Test readDirAt with different parameters
		entries, token, err := prefix.readDirAt(5, "", "", "")
		assert.NoError(t, err)
		assert.Len(t, entries, 5)
		_ = token // Ignore token for this test

		// Test readDirAt with seek
		entries, _, err = prefix.readDirAt(3, "", "file05.txt", "")
		assert.NoError(t, err)
		// Should return files after file05.txt
		if len(entries) > 0 {
			assert.True(t, entries[0].Name() > "file05.txt")
		}

		// Test readDirAt with pattern
		entries, _, err = prefix.readDirAt(10, "", "", "file0[0-2].txt")
		if err != io.EOF {
			assert.NoError(t, err)
			// Should only return files matching pattern
			for _, entry := range entries {
				matched, _ := patmatch("file0[0-2].txt", entry.Name())
				assert.True(t, matched)
			}
		}
	})

}

func TestListQuery(t *testing.T) {
	server := mock.New("test-bucket", "us-east-1")
	defer server.Close()
	key := aws.DeriveKey("", "test", "test", "us-east-1", "s3")
	key.BaseURI = server.URL()
	prefix := &Prefix{Key: key, Bucket: "test-bucket", Path: "dir/"}

	_, err := prefix.list(7, "next+ page", "file 2", "file")
	require.NoError(t, err)
	requests := server.GetRequestLog()
	require.Len(t, requests, 1)
	assert.Equal(t, "continuation-token=next%2B+page&delimiter=%2F&list-type=2&max-keys=7&prefix=dir%2Ffile&start-after=dir%2Ffile%202", requests[0].Query)
}

func BenchmarkListXMLDecode(b *testing.B) {
	for _, count := range []int{100, 1000} {
		var fixture bytes.Buffer
		fixture.WriteString("<ListBucketResult><IsTruncated>false</IsTruncated>")
		for i := range count {
			fmt.Fprintf(&fixture, "<Contents><Key>folder/file-%04d.txt</Key><LastModified>2026-01-02T03:04:05.000Z</LastModified><ETag>etag</ETag><Size>1</Size></Contents>", i)
		}
		fixture.WriteString("</ListBucketResult>")
		data := fixture.Bytes()
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				result, err := decodeListResponse(data)
				switch {
				case err != nil:
					b.Fatal(err)
				case len(result.Contents) != count:
					b.Fatalf("decoded %d entries, want %d", len(result.Contents), count)
				}
			}
		})
	}
}

func TestListResponseDecode(t *testing.T) {
	tests := []struct {
		name string
		xml  string
	}{
		{name: "empty", xml: `<ListBucketResult/>`},
		{name: "objects and prefixes", xml: `<ListBucketResult xmlns="urn:s3"><IsTruncated>true</IsTruncated><Contents><Key>folder/a&amp;b.txt</Key><LastModified>2026-01-02T03:04:05.000Z</LastModified><ETag>&quot;e&quot;</ETag><Size>12</Size><StorageClass>STANDARD</StorageClass></Contents><CommonPrefixes><Prefix>folder/sub/</Prefix></CommonPrefixes><EncodingType>url</EncodingType><NextContinuationToken>next&amp;page</NextContinuationToken><Owner><ID>ignored</ID></Owner></ListBucketResult>`},
		{name: "reordered fields", xml: `<ListBucketResult><NextContinuationToken>token</NextContinuationToken><Contents><Size>0</Size><ETag>etag</ETag><Key>key</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>`},
		{name: "numeric entities", xml: `<ListBucketResult><Contents><Key>emoji-&#x1F600;-&#38;.txt</Key></Contents></ListBucketResult>`},
		{name: "encoded scalar fallback", xml: `<ListBucketResult><IsTruncated>&#116;rue</IsTruncated><Contents><Key>key</Key><LastModified>2026-01-02T03:04:05&#90;</LastModified><Size>&#49;</Size></Contents></ListBucketResult>`},
		{name: "xml declaration", xml: `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult/>`},
		{name: "comment fallback", xml: `<ListBucketResult><!--page--><Contents><Key>key</Key></Contents></ListBucketResult>`},
		{name: "prefixed fallback", xml: `<s:ListBucketResult xmlns:s="urn:s3"><s:Contents><s:Key>key</s:Key></s:Contents></s:ListBucketResult>`},
		{name: "invalid declaration", xml: `<?xml garbage?><ListBucketResult/>`},
		{name: "invalid attribute", xml: `<ListBucketResult xmlns!="urn:s3"/>`},
		{name: "bad scalar", xml: `<ListBucketResult><Contents><Size>bad</Size></Contents></ListBucketResult>`},
		{name: "truncated", xml: `<ListBucketResult><Contents><Key>key</Key></Contents>`},
	}
	type standardListResponse listResponse
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeListResponse([]byte(test.xml))
			var want listResponse
			wantErr := xml.Unmarshal([]byte(test.xml), (*standardListResponse)(&want))
			assert.Equal(t, wantErr != nil, err != nil)
			if err == nil && wantErr == nil {
				assert.Equal(t, want, got)
			}
		})
	}
}

func TestListResponseOwnership(t *testing.T) {
	data := []byte(`<ListBucketResult><Contents><Key>file.txt</Key><ETag>&quot;tag&quot;</ETag></Contents><CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes><NextContinuationToken>next</NextContinuationToken></ListBucketResult>`)
	result, err := decodeListResponse(data)
	require.NoError(t, err)
	require.Len(t, result.Contents, 1)
	require.Len(t, result.CommonPrefixes, 1)
	for i := range data {
		data[i] = 'x'
	}
	assert.Equal(t, "file.txt", result.Contents[0].Path())
	assert.Equal(t, `"tag"`, result.Contents[0].ETag)
	assert.Equal(t, "dir/", result.CommonPrefixes[0].Path)
	assert.Equal(t, "next", result.NextToken)
}

func TestListBatchOwnership(t *testing.T) {
	var body bytes.Buffer
	body.WriteString("<ListBucketResult>")
	for i := range 130 {
		fmt.Fprintf(&body, "<Contents><ETag>&quot;tag-%d&quot;</ETag><Key>dir/file-%04d%s&amp;x</Key><Size>%d</Size></Contents>", i, i, strings.Repeat("x", i%23), i)
	}
	body.WriteString("</ListBucketResult>")
	data := body.Bytes()
	var want standardListResponse
	require.NoError(t, xml.Unmarshal(data, &want))
	got, err := decodeListResponse(data)
	require.NoError(t, err)
	assert.Equal(t, listResponse(want), got)
	clear(data)
	assert.Equal(t, listResponse(want), got)
}

func FuzzListResponseDecode(f *testing.F) {
	f.Add([]byte(`<ListBucketResult><Contents><Key>key</Key><Size>1</Size></Contents></ListBucketResult>`))
	f.Add([]byte(`<ListBucketResult><CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes></ListBucketResult>`))
	f.Add([]byte(`<ListBucketResult><IsTruncated>invalid</IsTruncated></ListBucketResult>`))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := decodeListResponse(data)
		var want standardListResponse
		wantErr := xml.Unmarshal(data, &want)
		assert.Equal(t, wantErr == nil, err == nil)
		if err == nil {
			assert.Equal(t, listResponse(want), got)
		}
	})
}
