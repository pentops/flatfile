package binfile

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pentops/flatfile/gen/flatfile/v1/flatfile_pb"
	"github.com/pentops/golib/gl"
	"github.com/pentops/j5/j5types/date_j5t"
	"github.com/pentops/j5/j5types/decimal_j5t"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func ParseMessage(msg proto.Message, data []byte) error {
	refl := msg.ProtoReflect()
	desc := refl.Descriptor()

	oneBased := false

	ext, ok := proto.GetExtension(desc.Options(), flatfile_pb.E_Message).(*flatfile_pb.Message)
	if ok && ext != nil {
		oneBased = ext.OneBased
	}

	rr := NewReader(data, oneBased)

	fields := desc.Fields()

	for i := range fields.Len() {
		fieldDesc := fields.Get(i)

		val, err := rr.ReadField(fieldDesc)
		if err != nil {
			return fmt.Errorf("error reading field %s: %w", fieldDesc.FullName(), err)
		}
		if val == nil {
			continue
		}
		refl.Set(fieldDesc, *val)

	}

	return nil
}

type Reader struct {
	Record   []byte
	OneBased bool
}

func NewReader(data []byte, oneBased bool) *Reader {
	return &Reader{
		Record:   data,
		OneBased: oneBased,
	}
}

func (r *Reader) getBytes(tc *flatfile_pb.Field) ([]byte, error) {
	offset := int(tc.FixedWidth.Offset)
	length := int(tc.FixedWidth.Length)
	if r.OneBased {
		offset = offset - 1
	}
	if offset+length > len(r.Record) {
		return nil, fmt.Errorf("short record")
	}
	return r.Record[offset : offset+length], nil
}

func (r *Reader) getString(tc *flatfile_pb.Field) (string, error) {
	byteVal, err := r.getBytes(tc)
	if err != nil {
		return "", err
	}
	return string(byteVal), nil
}

func (r *Reader) getNumberString(tc *flatfile_pb.Field) (string, error) {
	strVal, err := r.getString(tc)
	if err != nil {
		return "", err

	}
	number := tc.GetNumber()
	if number == nil {
		return strings.TrimSpace(strVal), nil
	}

	switch number.Encoding {
	case flatfile_pb.Encoding_ENCODING_UNSPECIFIED:
		return strings.TrimSpace(strVal), nil
	case flatfile_pb.Encoding_ENCODING_PACKED_DECIMAL:
		strVal, err = UnpackPacked([]byte(strVal))
		if err != nil {
			return "", fmt.Errorf("error unpacking packed decimal: %w", err)
		}
		return strVal, nil
	case flatfile_pb.Encoding_ENCODING_OVERPUNCH:
		strVal, err = DecodeOverpunch([]byte(strVal))
		if err != nil {
			return "", fmt.Errorf("error decoding overpunch decimal: %w", err)
		}
		return strVal, nil
	default:
		return "", fmt.Errorf("unknown number encoding %d", number.Encoding)
	}
}

func (r *Reader) ReadField(fieldDesc protoreflect.FieldDescriptor) (*protoreflect.Value, error) {
	tc := proto.GetExtension(fieldDesc.Options(), flatfile_pb.E_Field).(*flatfile_pb.Field)
	if tc == nil {
		return nil, nil
	}
	if tc.FixedWidth == nil {
		return nil, nil
	}

	switch fieldDesc.Kind() {
	case protoreflect.MessageKind:
		switch fieldDesc.Message().FullName() {
		case "google.protobuf.StringValue":
			return r.readStringValue(tc)
		case "google.protobuf.BoolValue":
			return r.readBoolValue(tc)
		case "j5.types.decimal.v1.Decimal":
			return r.readDecimal(tc)
		case "j5.types.date.v1.Date":
			return r.readDate(tc)
		default:
			return nil, fmt.Errorf("unknown struct type %s", fieldDesc.Message().FullName())
		}

	case protoreflect.StringKind:
		return r.readString(tc)

	case protoreflect.BoolKind:
		return r.readBoolValue(tc)

	case protoreflect.EnumKind:
		return r.readEnum(tc, fieldDesc.Enum())

	case protoreflect.Uint32Kind:
		return r.readUint32(tc)

	case protoreflect.Uint64Kind:
		return r.readUint64(tc)

	case protoreflect.Int32Kind:
		return r.readInt32(tc)

	case protoreflect.Int64Kind:
		return r.readInt64(tc)

	default:
		return nil, fmt.Errorf("unknown type/kind: %s", fieldDesc.Kind())
	}
}

func trimString(str string, tc *flatfile_pb.Field) string {
	stringField := tc.GetString_()
	if stringField == nil {
		return str
	}

	trimChars := stringField.TrimChars
	if trimChars == "" {
		trimChars = " "
	}

	switch stringField.Trim {
	case flatfile_pb.Trim_TRIM_UNSPECIFIED:
		return str
	case flatfile_pb.Trim_TRIM_LEFT:
		return strings.TrimLeft(str, trimChars)
	case flatfile_pb.Trim_TRIM_RIGHT:
		return strings.TrimRight(str, trimChars)
	case flatfile_pb.Trim_TRIM_BOTH:
		return strings.Trim(str, trimChars)
	default:
		return str
	}
}

func (r *Reader) readString(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	strVal, err := r.getString(tc)
	if err != nil {
		return nil, err
	}
	strVal = trimString(strVal, tc)

	return gl.Ptr(protoreflect.ValueOfString(strVal)), nil
}

func (r *Reader) readStringValue(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	strVal, err := r.getString(tc)
	if err != nil {
		return nil, err
	}
	strVal = trimString(strVal, tc)
	if strVal == "" {
		return nil, nil
	}
	return gl.Ptr(protoreflect.ValueOfMessage((&wrapperspb.StringValue{Value: strVal}).ProtoReflect())), nil
}

var (
	ErrMissingBool = errors.New("missing bool value")
)

func (r *Reader) readBoolValue(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	strVal, err := r.getString(tc)
	if err != nil {
		return nil, err
	}

	boolField := tc.GetBool()

	if boolField == nil {
		boolField = &flatfile_pb.BoolField{
			TrueValues:     []string{"T", "t", "Y", "y", "1"},
			FalseValues:    []string{"F", "f", "N", "n", "0"},
			TreatMissingAs: flatfile_pb.MissingIs_MISSING_IS_ERROR,
		}
	}

	if slices.Contains(boolField.TrueValues, strVal) {
		return gl.Ptr(protoreflect.ValueOf(true)), nil
	}
	if slices.Contains(boolField.FalseValues, strVal) {
		return gl.Ptr(protoreflect.ValueOf(false)), nil
	}

	switch boolField.TreatMissingAs {
	case flatfile_pb.MissingIs_MISSING_IS_UNSPECIFIED, flatfile_pb.MissingIs_MISSING_IS_FALSE:
		return gl.Ptr(protoreflect.ValueOfBool(false)), nil
	case flatfile_pb.MissingIs_MISSING_IS_TRUE:
		return gl.Ptr(protoreflect.ValueOfBool(true)), nil
	case flatfile_pb.MissingIs_MISSING_IS_ERROR:
		return nil, ErrMissingBool
	default:
		return nil, fmt.Errorf("unknown missing value def for bool")
	}
}

func (r *Reader) readDecimal(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	stringVal, err := r.getNumberString(tc)
	if err != nil {
		return nil, err
	}
	if stringVal == "" {
		return nil, nil
	}
	val, err := decimal.NewFromString(stringVal)
	if err != nil {
		return nil, fmt.Errorf("invalid decimal value: %q", stringVal)
	}
	msgVal := decimal_j5t.FromShop(val)
	return gl.Ptr(protoreflect.ValueOfMessage(msgVal.ProtoReflect())), nil
}

var reNumbers = regexp.MustCompile(`[MDY]`)

func goTimeFormat(a string) (string, error) {
	a = strings.Replace(a, "YYYY", "2006", 1)
	a = strings.Replace(a, "YY", "06", 1)
	a = strings.Replace(a, "MM", "01", 1)
	a = strings.Replace(a, "DD", "02", 1)
	return a, nil
}

func (r *Reader) readDate(tc *flatfile_pb.Field) (*protoreflect.Value, error) {

	dateField := tc.GetDate()
	if dateField == nil || dateField.Format == "" {
		return nil, fmt.Errorf("missing date format for date field")
	}

	stringVal, err := r.getString(tc)
	if err != nil {
		return nil, err
	}

	layout, err := goTimeFormat(dateField.Format)
	if err != nil {
		return nil, fmt.Errorf("invalid time layout: %s", dateField.Format)
	}

	emptyVals := []string{
		strings.Repeat(" ", len(dateField.Format)),
		reNumbers.ReplaceAllString(dateField.Format, "0"),
		reNumbers.ReplaceAllString(dateField.Format, " "),
	}

	emptyVals = append(emptyVals, dateField.ZeroVals...)

	if slices.Contains(emptyVals, stringVal) {
		return nil, nil
	}

	timeVal, err := time.Parse(layout, stringVal)
	if err != nil {
		return nil, fmt.Errorf("invalid date value: %s", stringVal)
	}

	yy, mm, dd := timeVal.Date()
	dateVal := &date_j5t.Date{
		Year:  int32(yy),
		Month: int32(mm),
		Day:   int32(dd),
	}
	return gl.Ptr(protoreflect.ValueOfMessage(dateVal.ProtoReflect())), nil
}

func (r *Reader) readEnum(tc *flatfile_pb.Field, enum protoreflect.EnumDescriptor) (*protoreflect.Value, error) {
	stringVal, err := r.getString(tc)
	if err != nil {
		return nil, err
	}

	values := enum.Values()
	for i := range values.Len() {
		valueDesc := values.Get(i)
		tc := proto.GetExtension(valueDesc.Options(), flatfile_pb.E_Enum).(*flatfile_pb.Enum)
		if tc == nil {
			continue
		}

		if strings.TrimSpace(tc.Key) == stringVal {
			return gl.Ptr(protoreflect.ValueOfEnum(valueDesc.Number())), nil
		}
	}

	if strings.TrimSpace(stringVal) == "" {
		return nil, nil
	}

	return nil, fmt.Errorf("invalid enum value: %q", stringVal)
}

func (r *Reader) unsignedStringNumber(tc *flatfile_pb.Field, size int) (uint64, bool, error) {
	numString, err := r.getNumberString(tc)
	if err != nil {
		return 0, false, err
	}
	numString = strings.TrimLeft(numString, " 0")
	if numString == "" {
		return 0, false, nil
	}
	val, err := strconv.ParseUint(numString, 10, size)
	if err != nil {
		return 0, false, fmt.Errorf("parsing %q as uint: %w", numString, err)
	}
	return val, true, nil
}

func (r *Reader) signedStringNumber(tc *flatfile_pb.Field, size int) (int64, bool, error) {
	numString, err := r.getNumberString(tc)
	if err != nil {
		return 0, false, err
	}
	numString = strings.TrimLeft(numString, " 0")
	if numString == "" {
		return 0, false, nil
	}
	val, err := strconv.ParseInt(numString, 10, size)
	if err != nil {
		return 0, false, fmt.Errorf("parsing %q as int: %w", numString, err)
	}
	return val, true, nil
}

func (r *Reader) leftPaddedBytes(tc *flatfile_pb.Field, typeLength int) ([]byte, error) {
	readLength := int(tc.FixedWidth.Length)
	if typeLength < readLength {
		return nil, fmt.Errorf("type length %d less than read length %d", typeLength, readLength)
	}

	byteVal, err := r.getBytes(tc)
	if err != nil {
		return nil, err
	}

	if typeLength == readLength {
		return byteVal, nil
	}

	newVal := make([]byte, typeLength)
	copy(newVal[readLength-len(byteVal):], byteVal)
	return newVal, nil
}

var overpunchVals = `{ABCDEFGHI}JKLMNOPQR`

func DecodeOverpunch(in []byte) (string, error) {
	last := in[len(in)-1]
	overpunchIndex := strings.IndexByte(overpunchVals, last)
	if overpunchIndex < 0 {
		return "", fmt.Errorf("invalid overpunch byte: %x", last)
	}
	out := []byte(in)
	out[len(in)-1] = byte(overpunchIndex%10 + 0x30)
	if overpunchIndex > 9 {
		return "-" + string(out), nil
	}
	return string(out), nil
}

// UnpackPacked unpacks a Packed Binary Coded Decimal from the source bytes
func UnpackPacked(in []byte) (string, error) {
	negative := false
	out := make([]byte, 0, len(in)*2)
	for idx, ab := range in {
		a := ab >> 4
		b := ab & 0x0f
		if idx == len(in)-1 {
			// Last Byte
			out = append(out, a)

			if b < 0x0A {
				// It's a number component, not a sign
				out = append(out, b)
			} else {
				// The last nibble is the sign
				// B and D indicate negative, the rest are either unsigned or
				// positive.
				if b == 0x0D || b == 0x0B {
					negative = true
				}
			}

		} else if idx == 0 && b >= 0x0a {
			// If the first char is outside of the 0-9 range, discard it
			out = append(out, a)
		} else {
			// TODO: Should this be (b, a) or (a, b)? (and other follow on effects)
			out = append(out, a, b)
		}
	}

	strOut := make([]byte, 0, len(out)+1)
	if negative {
		strOut = append(strOut, '-')
	}

	hadAny := false
	for _, b := range out {
		if b == 0x00 && !hadAny {
			continue
		}
		hadAny = true
		strOut = append(strOut, byte(b+0x30)) // 0x30 is the CHARACTER '0'
	}

	vv := string(strOut)
	return vv, nil
}

func numberFormat(tc *flatfile_pb.Field) flatfile_pb.Encoding {
	numberField := tc.GetNumber()
	if numberField != nil && numberField.Encoding != flatfile_pb.Encoding_ENCODING_UNSPECIFIED {
		return numberField.Encoding
	}
	return flatfile_pb.Encoding_ENCODING_UNSPECIFIED
}

func (r *Reader) readUint32(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	format := numberFormat(tc)
	if format == flatfile_pb.Encoding_ENCODING_BINARY {
		byteVal, err := r.leftPaddedBytes(tc, 32/8)
		if err != nil {
			return nil, err
		}
		val := byteVal[0]
		return gl.Ptr(protoreflect.ValueOfUint32(uint32(val))), nil
	}

	val, isSet, err := r.unsignedStringNumber(tc, 32)
	if err != nil {
		return nil, err
	}
	if !isSet {
		return nil, nil
	}
	return gl.Ptr(protoreflect.ValueOfUint32(uint32(val))), nil
}

func (r *Reader) readUint64(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	format := numberFormat(tc)
	if format == flatfile_pb.Encoding_ENCODING_BINARY {
		byteVal, err := r.leftPaddedBytes(tc, 64/8)
		if err != nil {
			return nil, err
		}
		val := byteVal[0]
		return gl.Ptr(protoreflect.ValueOfUint64(uint64(val))), nil
	}

	val, isSet, err := r.unsignedStringNumber(tc, 64)
	if err != nil {
		return nil, err
	}
	if !isSet {
		return nil, nil
	}
	return gl.Ptr(protoreflect.ValueOfUint64(val)), nil
}

func (r *Reader) readInt32(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	format := numberFormat(tc)
	if format == flatfile_pb.Encoding_ENCODING_BINARY {
		byteVal, err := r.leftPaddedBytes(tc, 32/8)
		if err != nil {
			return nil, err
		}
		val := byteVal[0]
		signedVal := int32(val)
		return gl.Ptr(protoreflect.ValueOfInt32(signedVal)), nil
	}

	val, isSet, err := r.signedStringNumber(tc, 32)
	if err != nil {
		return nil, err
	}
	if !isSet {
		return nil, nil
	}
	return gl.Ptr(protoreflect.ValueOfInt32(int32(val))), nil
}

func (r *Reader) readInt64(tc *flatfile_pb.Field) (*protoreflect.Value, error) {
	format := numberFormat(tc)
	if format == flatfile_pb.Encoding_ENCODING_BINARY {
		byteVal, err := r.leftPaddedBytes(tc, 64/8)
		if err != nil {
			return nil, err
		}
		val := byteVal[0]
		signedVal := int64(val)
		return gl.Ptr(protoreflect.ValueOfInt64(signedVal)), nil
	}

	val, isSet, err := r.signedStringNumber(tc, 64)
	if err != nil {
		return nil, err
	}
	if !isSet {
		return nil, nil
	}
	return gl.Ptr(protoreflect.ValueOfInt64(val)), nil
}
