// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"

	jsoniter "github.com/json-iterator/go"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

type mappingModel interface {
	encodeLog(pcommon.Resource, plog.LogRecord, pcommon.InstrumentationScope) ([]byte, error)
	encodeMetric(pcommon.Resource, pmetric.MetricSlice, pcommon.InstrumentationScope) ([]bytes.Buffer, error)
	encodeSpan(pcommon.Resource, ptrace.Span, pcommon.InstrumentationScope) ([]byte, error)
}

// encodeModel tries to keep the event as close to the original open telemetry semantics as is.
// No fields will be mapped by default.
//
// Field deduplication and dedotting of attributes is supported by the encodeModel.
//
// See: https://github.com/open-telemetry/oteps/blob/master/text/logs/0097-log-data-model.md
type encodeModel struct {
	dedup   bool
	dedot   bool
	mapping string
}

const (
	traceIDField   = "traceID"
	spanIDField    = "spanID"
	attributeField = "attribute"
)

func sameResourceCheck(left []attribute.KeyValue, right []attribute.KeyValue) bool {
	for _, leftAttr := range left {
		for _, rightAttr := range right {
			if leftAttr.Key == rightAttr.Key {
				if leftAttr.Value != rightAttr.Value {
					return false
				}
			}
		}
	}
	return true
}

func hashAttributes(metricAttributes pcommon.Map) (int32, error) {
	hasher := fnv.New32a()
	marshaler := jsoniter.ConfigCompatibleWithStandardLibrary

	data, err := marshaler.Marshal(metricAttributes.AsRaw())
	if err != nil {
		return 0, err
	}
	_, err = hasher.Write(data)
	if err != nil {
		return 0, err
	}

	hashed := hasher.Sum32()
	hasher.Reset()

	return int32(hashed), nil
}

func tryGetNewDocument(idToDoc map[int32]*objmodel.Document, attributes pcommon.Map, resource pcommon.Resource, timestamp pcommon.Timestamp) *objmodel.Document {

	id, err := hashAttributes(attributes)

	if err != nil {
	}

	val, ok := idToDoc[id]
	if !ok {
		var document objmodel.Document
		document.AddAttributes("", attributes)
		document.AddAttributes("", resource.Attributes())
		document.AddTimestamp("@timestamp", timestamp)
		idToDoc[id] = &document
		return &document
	} else {
		return val
	}
}

func (m *encodeModel) encodeMetric(resource pcommon.Resource, slice pmetric.MetricSlice, scope pcommon.InstrumentationScope) ([]bytes.Buffer, error) {

	documents := make(map[int32]*objmodel.Document)

	/* Read through our slice of metrics and merge metrics into a common document when their attributes are identical */
	for k := 0; k < slice.Len(); k++ {
		thisMetric := slice.At(k)

		switch thisMetric.Type() {
		case pmetric.MetricTypeGauge:
			dataPts := thisMetric.Gauge().DataPoints()

			for i := 0; i < dataPts.Len(); i++ {
				thisDataPt := dataPts.At(i)

				thisDocument := *tryGetNewDocument(documents, thisDataPt.Attributes(), resource, thisDataPt.Timestamp())

				switch thisDataPt.ValueType() {
				case pmetric.NumberDataPointValueTypeInt:
					thisDocument.AddAttribute(thisMetric.Name(), pcommon.NewValueInt(thisDataPt.IntValue()))
				case pmetric.NumberDataPointValueTypeDouble:
					thisDocument.AddAttribute(thisMetric.Name(), pcommon.NewValueDouble(thisDataPt.DoubleValue()))
				}
			}

		case pmetric.MetricTypeSum:
			dataPts := thisMetric.Sum().DataPoints()

			for i := 0; i < dataPts.Len(); i++ {
				thisDataPt := dataPts.At(i)

				thisDocument := tryGetNewDocument(documents, thisDataPt.Attributes(), resource, thisDataPt.Timestamp())

				switch thisDataPt.ValueType() {
				case pmetric.NumberDataPointValueTypeInt:
					thisDocument.AddAttribute(thisMetric.Name(), pcommon.NewValueInt(thisDataPt.IntValue()))
				case pmetric.NumberDataPointValueTypeDouble:
					thisDocument.AddAttribute(thisMetric.Name(), pcommon.NewValueDouble(thisDataPt.DoubleValue()))
				}
			}
		}
	}

	var buffers []bytes.Buffer
	var err error

	for _, doc := range documents {
		if m.dedup {
			doc.Dedup()
		} else if m.dedot {
			doc.Sort()
		}

		var buf bytes.Buffer
		err = doc.Serialize(&buf, m.dedot)

		buffers = append(buffers, buf)
	}
	return buffers, err
}

func (m *encodeModel) encodeLog(resource pcommon.Resource, record plog.LogRecord, scope pcommon.InstrumentationScope) ([]byte, error) {
	var document objmodel.Document
	document.AddTimestamp("@timestamp", record.Timestamp()) // We use @timestamp in order to ensure that we can index if the default data stream logs template is used.

	if m.mapping == MappingECS.String() {
		/* Add message first and overwrite it from the record if present */
		document.AddAttribute("message", record.Body())

		document.AddAttributes("", record.Attributes())
		document.AddAttributes("", resource.Attributes())

		document.AddAttribute("log.level", pcommon.NewValueStr(record.SeverityText()))
	} else {
		document.AddTraceID("TraceId", record.TraceID())
		document.AddSpanID("SpanId", record.SpanID())
		document.AddInt("TraceFlags", int64(record.Flags()))
		document.AddString("SeverityText", record.SeverityText())
		document.AddInt("SeverityNumber", int64(record.SeverityNumber()))
		document.AddAttribute("Body", record.Body())
		document.AddAttributes("Attributes", record.Attributes())
		document.AddAttributes("Resource", resource.Attributes())
		document.AddAttributes("Scope", scopeToAttributes(scope))
	}

	if m.dedup {
		document.Dedup()
	} else if m.dedot {
		document.Sort()
	}

	var buf bytes.Buffer
	err := document.Serialize(&buf, m.dedot)
	return buf.Bytes(), err
}

func (m *encodeModel) encodeSpan(resource pcommon.Resource, span ptrace.Span, scope pcommon.InstrumentationScope) ([]byte, error) {
	var document objmodel.Document
	document.AddTimestamp("@timestamp", span.StartTimestamp()) // We use @timestamp in order to ensure that we can index if the default data stream logs template is used.
	document.AddTimestamp("EndTimestamp", span.EndTimestamp())
	document.AddTraceID("TraceId", span.TraceID())
	document.AddSpanID("SpanId", span.SpanID())
	document.AddSpanID("ParentSpanId", span.ParentSpanID())
	document.AddString("Name", span.Name())
	document.AddString("Kind", traceutil.SpanKindStr(span.Kind()))
	document.AddInt("TraceStatus", int64(span.Status().Code()))
	document.AddString("Link", spanLinksToString(span.Links()))
	document.AddAttributes("Attributes", span.Attributes())
	document.AddAttributes("Resource", resource.Attributes())
	document.AddEvents("Events", span.Events())
	document.AddInt("Duration", durationAsMicroseconds(span.StartTimestamp().AsTime(), span.EndTimestamp().AsTime())) // unit is microseconds
	document.AddAttributes("Scope", scopeToAttributes(scope))

	if m.dedup {
		document.Dedup()
	} else if m.dedot {
		document.Sort()
	}

	var buf bytes.Buffer
	err := document.Serialize(&buf, m.dedot)
	return buf.Bytes(), err
}

func spanLinksToString(spanLinkSlice ptrace.SpanLinkSlice) string {
	linkArray := make([]map[string]interface{}, 0, spanLinkSlice.Len())
	for i := 0; i < spanLinkSlice.Len(); i++ {
		spanLink := spanLinkSlice.At(i)
		link := map[string]interface{}{}
		link[spanIDField] = traceutil.SpanIDToHexOrEmptyString(spanLink.SpanID())
		link[traceIDField] = traceutil.TraceIDToHexOrEmptyString(spanLink.TraceID())
		link[attributeField] = spanLink.Attributes().AsRaw()
		linkArray = append(linkArray, link)
	}
	linkArrayBytes, _ := json.Marshal(&linkArray)
	return string(linkArrayBytes)
}

// durationAsMicroseconds calculate span duration through end - start nanoseconds and converts time.Time to microseconds,
// which is the format the Duration field is stored in the Span.
func durationAsMicroseconds(start, end time.Time) int64 {
	return (end.UnixNano() - start.UnixNano()) / 1000
}

func scopeToAttributes(scope pcommon.InstrumentationScope) pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("name", scope.Name())
	attrs.PutStr("version", scope.Version())
	for k, v := range scope.Attributes().AsRaw() {
		attrs.PutStr(k, v.(string))
	}
	return attrs
}
