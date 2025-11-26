mkdir -p ./base
mkdir -p ./common
protoc --go_out=./base --go_opt=paths=source_relative --mgorm_out=./base --mgorm_opt=paths=source_relative --proto_path=./proto ./proto/base.proto
protoc --go_out=./common --go_opt=paths=source_relative --mgorm_out=./common --mgorm_opt=paths=source_relative --proto_path=./proto ./proto/comm.proto

if [ $# -eq 0 ]; then
  exit 0
fi

mkdir -p ./$1
protoc --go_out=./$1 --go_opt=paths=source_relative --mgorm_out=./$1 --mgorm_opt=paths=source_relative --proto_path=./proto ./proto/$1.proto
