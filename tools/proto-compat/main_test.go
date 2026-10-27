package main

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"testing"
)

func TestOnlyHistoricalGoPackageIsNormalized(t *testing.T) {
	old := "pintle.local/spike/gen/go/pintle/v1;pintlev1"
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{
		{Name: proto.String("pintle/v1/lock.proto"), Options: &descriptorpb.FileOptions{GoPackage: &old}, MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("KeepMe")}}},
		{Name: proto.String("other.proto"), Options: &descriptorpb.FileOptions{GoPackage: &old}},
	}}
	want := proto.Clone(set).(*descriptorpb.FileDescriptorSet)
	want.File[0].Options.GoPackage = proto.String("github.com/Noot-tooN/Pintle/gen/go/pintle/v1;pintlev1")
	normalize(set)
	if !proto.Equal(set, want) {
		t.Fatal("changed more than historical Go import metadata")
	}
	set.File[0].Options.GoPackage = proto.String("another.example/changed")
	want = proto.Clone(set).(*descriptorpb.FileDescriptorSet)
	normalize(set)
	if !proto.Equal(set, want) {
		t.Fatal("masked a different import-path change")
	}
}
