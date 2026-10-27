// Normalize only the unpublished spike's Go import path before compatibility
// comparison. Keep every field, method and other option in the baseline intact.
package main

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func normalize(set *descriptorpb.FileDescriptorSet) {
	for _, f := range set.File {
		if f.GetName() == "pintle/v1/lock.proto" && f.GetOptions().GetGoPackage() == "pintle.local/spike/gen/go/pintle/v1;pintlev1" {
			f.Options.GoPackage = proto.String("github.com/Noot-tooN/Pintle/gen/go/pintle/v1;pintlev1")
		}
	}
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: proto-compat input-descriptor output-descriptor")
	}
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(b, &set); err != nil {
		return err
	}
	normalize(&set)
	b, err = proto.Marshal(&set)
	if err != nil {
		return err
	}
	return os.WriteFile(os.Args[2], b, 0600)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
