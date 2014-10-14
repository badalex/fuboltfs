# pacman -S protobuf
# go get code.google.com/p/goprotobuf/{proto,protoc-gen-go}

fuboltfs: *.go wfile.pb.go Makefile
	go build .

wfile.pb.go: wfile.proto
	protoc --go_out=. *.proto

clean:
	rm -f wfile.pb.go fuboltfs

