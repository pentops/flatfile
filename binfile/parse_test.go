package binfile

import (
	"errors"
	"strings"
	"testing"

	"github.com/pentops/flowtest/prototest"
	"github.com/pentops/j5/lib/j5codec"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestMultiType(t *testing.T) {

	fileDesc := prototest.DescriptorsFromSource(t, map[string]string{"test.proto": `
		syntax = "proto3";
		package bar.v1;

		import "flatfile/v1/annotations.proto";
		import "google/protobuf/wrappers.proto";
		import "j5/types/date/v1/date.proto";
		import "j5/types/decimal/v1/decimal.proto";

		message Record {
		  option (flatfile.v1.message).one_based = true;

		  RecordType record_type = 1 [(flatfile.v1.field) = {
			fixed_width: {
			  offset: 1
			  length: 1
			}
		  }];

		  j5.types.date.v1.Date file_creation_date = 2 [(flatfile.v1.field) = {
			fixed_width: {
			  offset: 2
			  length: 10
			}
			date: {format: "YYYY-MM-DD"}
		  }];

		  string str = 3 [(flatfile.v1.field) = {
			fixed_width: {
			  offset: 12
			  length: 5
			}
			string: {
			  trim: TRIM_BOTH
			}
		  }];

		  bool flagged = 4 [(flatfile.v1.field) = {
			fixed_width: {
			  offset: 17
			  length: 1
			}
			bool: {
			  true_values: ["X"]
			  false_values: [" "]
			  treat_missing_as: MISSING_IS_ERROR
			}
		  }];
		}

		enum RecordType {
		  RECORD_TYPE_UNSPECIFIED = 0;
		  RECORD_TYPE_FOO = 1 [(flatfile.v1.enum).key = "F"];
		  RECORD_TYPE_BAR = 2 [(flatfile.v1.enum).key = "B"];
		}`})

	msgDesc := fileDesc.MessageByName(t, "bar.v1.Record")

	t.Run("Full Valid", func(t *testing.T) {
		runCmp(t, msgDesc, []string{
			"F",
			"2003-01-02",
			"12345",
			"X",
		}, `{
			"recordType": "FOO",
			"fileCreationDate": "2003-01-02",
			"str": "12345",
			"flagged": true
		}`)
	})

	t.Run("Empty Valid", func(t *testing.T) {
		runCmp(t, msgDesc, []string{
			" ",
			"          ",
			"     ",
			" ",
		}, `{
			"recordType": "RECORD_TYPE_UNSPECIFIED",
			"fileCreationDate": null,
			"str": "",
			"flagged": false
		}`)
	})
}

func TestTypes(t *testing.T) {

	t.Run("Invalid Bool", func(t *testing.T) {
		msgDesc := prototest.SingleMessage(t, `
		  bool flagged = 4 [(flatfile.v1.field) = {
			fixed_width: {
			  offset: 0
			  length: 1
			}
			bool: {
			  true_values: ["X"]
			  false_values: [" "]
			  treat_missing_as: MISSING_IS_ERROR
			}
		  }];
		  `)

		err := runErr(t, msgDesc, []string{"Y"})

		if !errors.Is(err, ErrMissingBool) {
			t.Fatalf("expected ErrMissingBool, got %v", err)
		}
	})

	t.Run("Bool Missing Is False", func(t *testing.T) {
		msgDesc := prototest.SingleMessage(t, `
		  bool flagged = 4 [(flatfile.v1.field) = {
			fixed_width: {
			  offset: 0
			  length: 1
			}
			bool: {
			  true_values: ["X"]
			  false_values: [" "]
			  treat_missing_as: MISSING_IS_FALSE
			}
		  }];
		  `)

		runCmp(t, msgDesc, []string{"#"}, `{ "flagged": false }`)
		runCmp(t, msgDesc, []string{" "}, `{ "flagged": false }`)
		runCmp(t, msgDesc, []string{"X"}, `{ "flagged": true }`)
	})

	t.Run("Decimal", func(t *testing.T) {
		msgDesc := prototest.SingleMessage(t,
			prototest.WithMessageImports("j5/types/decimal/v1/decimal.proto"),
			`
		  j5.types.decimal.v1.Decimal amount = 1 [(flatfile.v1.field) = {
			fixed_width: { offset: 0, length: 10 }
			number: { }
		  }];
		  `)

		runCmp(t, msgDesc, []string{"0000123.45"}, `{ "amount": "123.45" }`)
		runCmp(t, msgDesc, []string{"    123.45"}, `{ "amount": "123.45" }`)
	})

	t.Run("StringValue", func(t *testing.T) {
		msgDesc := prototest.SingleMessage(t,
			prototest.WithMessageImports("google/protobuf/wrappers.proto"),
			`
		  google.protobuf.StringValue note = 1 [(flatfile.v1.field) = {
			fixed_width: { offset: 0, length: 5 }
			string: { trim: TRIM_BOTH }
		  }];
		  `)

		record := dynamicpb.NewMessage(msgDesc)
		err := ParseMessage(record, []byte("abc  "))
		if err != nil {
			t.Fatalf("error parsing record: %v", err)
		}

		noteField := msgDesc.Fields().ByName("note")
		if !record.Has(noteField) {
			t.Fatalf("expected note field to be set")
		}

		want := wrapperspb.String("abc")
		got := record.Get(noteField).Message().Interface()
		prototest.AssertEqualProto(t, want, got)

		record = dynamicpb.NewMessage(msgDesc)
		err = ParseMessage(record, []byte("     "))
		if err != nil {
			t.Fatalf("error parsing record: %v", err)
		}
		if record.Has(noteField) {
			t.Fatalf("expected note field to be unset for empty value")
		}
	})

	t.Run("Numeric Types String Encoded", func(t *testing.T) {
		msgDesc := prototest.SingleMessage(t, `
		  uint32 u32 = 1 [(flatfile.v1.field) = {
			fixed_width: { offset: 0, length: 4 }
			number: {}
		  }];
		  uint64 u64 = 2 [(flatfile.v1.field) = {
			fixed_width: { offset: 4, length: 4 }
			number: {}
		  }];
		  int32 i32 = 3 [(flatfile.v1.field) = {
			fixed_width: { offset: 8, length: 4 }
			number: {}
		  }];
		  int64 i64 = 4 [(flatfile.v1.field) = {
			fixed_width: { offset: 12, length: 4 }
			number: {}
		  }];
		`)

		runCmp(t, msgDesc, []string{"0042", "1234", "-012", "0010"}, `{
			"u32": 42,
			"u64": "1234",
			"i32": -12,
			"i64": "10"
		}`)

		runCmp(t, msgDesc, []string{"    ", "    ", "    ", "    "}, `{}`)
	})

	t.Run("Numeric Types Binary Encoded", func(t *testing.T) {
		msgDesc := prototest.SingleMessage(t, `
		  uint32 u32 = 1 [(flatfile.v1.field) = {
			fixed_width: { offset: 0, length: 1 }
			number: { encoding: ENCODING_BINARY }
		  }];
		  uint64 u64 = 2 [(flatfile.v1.field) = {
			fixed_width: { offset: 1, length: 1 }
			number: { encoding: ENCODING_BINARY }
		  }];
		  int32 i32 = 3 [(flatfile.v1.field) = {
			fixed_width: { offset: 2, length: 1 }
			number: { encoding: ENCODING_BINARY }
		  }];
		  int64 i64 = 4 [(flatfile.v1.field) = {
			fixed_width: { offset: 3, length: 1 }
			number: { encoding: ENCODING_BINARY }
		  }];
		`)

		runCmp(t, msgDesc, []string{"\x2a", "\xff", "\x7f", "\x80"}, `{
			"u32": 42,
			"u64": "255",
			"i32": 127,
			"i64": "128"
		}`)
	})

}

func runErr(t testing.TB, msgDesc protoreflect.MessageDescriptor, in []string) error {
	t.Helper()
	line := strings.Join(in, "")
	record := dynamicpb.NewMessage(msgDesc)

	err := ParseMessage(record, []byte(line))
	if err == nil {
		t.Fatalf("expected error parsing record, got nil")
	}

	return err
}

func runCmp(t testing.TB, msgDesc protoreflect.MessageDescriptor, in []string, wantJSON string) {
	t.Helper()
	line := strings.Join(in, "")
	record := dynamicpb.NewMessage(msgDesc)

	err := ParseMessage(record, []byte(line))
	if err != nil {
		t.Fatalf("error parsing record: %v", err)
	}

	want := dynamicpb.NewMessage(msgDesc)
	err = j5codec.Global.JSONToProto([]byte(wantJSON), want)
	if err != nil {
		t.Fatalf("error unmarshaling expected record: %v", err)
	}

	prototest.AssertEqualProto(t, want, record)

}
