mkdir -p ./pb
protoc --go_out=./pb --go_opt=paths=source_relative --proto_path=./proto ./proto/mgorm_ext.proto

