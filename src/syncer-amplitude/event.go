package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/BemiHQ/BemiDB/src/syncer-common"
)

const (
	EVENTS_TABLE_NAME = "events"
)

type Event struct {
	Adid                    string                 `json:"adid"`
	AmplitudeAttributionIDs interface{}            `json:"amplitude_attribution_ids"`
	AmplitudeEventType      interface{}            `json:"amplitude_event_type"`
	AmplitudeID             int64                  `json:"amplitude_id"`
	App                     int                    `json:"app"`
	City                    string                 `json:"city"`
	ClientEventTime         string                 `json:"client_event_time"`
	ClientUploadTime        string                 `json:"client_upload_time"`
	Country                 string                 `json:"country"`
	Data                    map[string]interface{} `json:"data"`
	DataType                string                 `json:"data_type"`
	DeviceBrand             string                 `json:"device_brand"`
	DeviceCarrier           string                 `json:"device_carrier"`
	DeviceFamily            string                 `json:"device_family"`
	DeviceID                string                 `json:"device_id"`
	DeviceManufacturer      string                 `json:"device_manufacturer"`
	DeviceModel             string                 `json:"device_model"`
	DeviceType              string                 `json:"device_type"`
	DMA                     string                 `json:"dma"`
	EventID                 int64                  `json:"event_id"`
	EventProperties         map[string]interface{} `json:"event_properties"`
	EventTime               string                 `json:"event_time"`
	EventType               string                 `json:"event_type"`
	GlobalUserProperties    map[string]interface{} `json:"global_user_properties"`
	GroupProperties         map[string]interface{} `json:"group_properties"`
	Groups                  map[string]interface{} `json:"groups"`
	IDFA                    string                 `json:"idfa"`
	InsertID                string                 `json:"$insert_id"`
	InsertKey               interface{}            `json:"$insert_key"`
	IPAddress               string                 `json:"ip_address"`
	IsAttributionEvent      interface{}            `json:"is_attribution_event"`
	Language                string                 `json:"language"`
	Library                 string                 `json:"library"`
	LocationLat             interface{}            `json:"location_lat"`
	LocationLng             interface{}            `json:"location_lng"`
	OSName                  string                 `json:"os_name"`
	OSVersion               string                 `json:"os_version"`
	PartnerID               interface{}            `json:"partner_id"`
	Paying                  interface{}            `json:"paying"`
	Plan                    map[string]interface{} `json:"plan"`
	Platform                string                 `json:"platform"`
	ProcessedTime           string                 `json:"processed_time"`
	Region                  string                 `json:"region"`
	SampleRate              interface{}            `json:"sample_rate"`
	Schema                  interface{}            `json:"$schema"`
	ServerReceivedTime      string                 `json:"server_received_time"`
	ServerUploadTime        string                 `json:"server_upload_time"`
	SessionID               int64                  `json:"session_id"`
	SourceID                interface{}            `json:"source_id"`
	StartVersion            string                 `json:"start_version"`
	UserCreationTime        string                 `json:"user_creation_time"`
	UserID                  string                 `json:"user_id"`
	UserProperties          map[string]interface{} `json:"user_properties"`
	UUID                    string                 `json:"uuid"`
	VersionName             string                 `json:"version_name"`
}

func (e *Event) ToTrinoRow() string {
	toVarchar := func(s string) string {
		if s == "" {
			return "NULL"
		}
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}

	toJsonVarchar := func(data map[string]interface{}) string {
		if len(data) == 0 {
			return "NULL"
		}
		bytes, err := json.Marshal(data)
		if err != nil {
			return "NULL"
		}
		return toVarchar(string(bytes))
	}

	toInterfaceVarchar := func(i interface{}) string {
		if i == nil {
			return "NULL"
		}
		s, ok := i.(string)
		if !ok {
			bytes, err := json.Marshal(i)
			if err != nil {
				return "NULL"
			}
			return toVarchar(string(bytes))
		}
		return toVarchar(s)
	}

	toFloat64String := func(f interface{}) string {
		if f == nil {
			return "NULL"
		}
		val, ok := f.(float64)
		if !ok {
			return "NULL"
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	}

	toBoolString := func(b interface{}) string {
		if b == nil {
			return "NULL"
		}
		val, ok := b.(bool)
		if !ok {
			return "NULL"
		}
		return strconv.FormatBool(val)
	}

	parseTime := func(t string) string {
		if t == "" {
			return "NULL"
		}
		parsedTime, err := time.Parse("2006-01-02 15:04:05.999999", t)
		if err != nil {
			parsedTime, err = time.Parse("2006-01-02 15:04:05", t)
			if err != nil {
				parsedTime, err = time.Parse(time.RFC3339, t)
				if err != nil {
					return "NULL"
				}
			}
		}
		return "TIMESTAMP '" + syncerCommon.TimeToUtcStringMs(parsedTime) + "'"
	}

	return fmt.Sprintf("(%s, %s, %s, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %s)",
		toVarchar(e.Adid),
		toInterfaceVarchar(e.AmplitudeAttributionIDs),
		toInterfaceVarchar(e.AmplitudeEventType),
		e.AmplitudeID,
		e.App,
		toVarchar(e.City),
		parseTime(e.ClientEventTime),
		parseTime(e.ClientUploadTime),
		toVarchar(e.Country),
		toJsonVarchar(e.Data),
		toVarchar(e.DataType),
		toVarchar(e.DeviceBrand),
		toVarchar(e.DeviceCarrier),
		toVarchar(e.DeviceFamily),
		toVarchar(e.DeviceID),
		toVarchar(e.DeviceManufacturer),
		toVarchar(e.DeviceModel),
		toVarchar(e.DeviceType),
		toVarchar(e.DMA),
		e.EventID,
		toJsonVarchar(e.EventProperties),
		parseTime(e.EventTime),
		toVarchar(e.EventType),
		toJsonVarchar(e.GlobalUserProperties),
		toJsonVarchar(e.GroupProperties),
		toJsonVarchar(e.Groups),
		toVarchar(e.IDFA),
		toVarchar(e.InsertID),
		toInterfaceVarchar(e.InsertKey),
		toVarchar(e.IPAddress),
		toBoolString(e.IsAttributionEvent),
		toVarchar(e.Language),
		toVarchar(e.Library),
		toFloat64String(e.LocationLat),
		toFloat64String(e.LocationLng),
		toVarchar(e.OSName),
		toVarchar(e.OSVersion),
		toInterfaceVarchar(e.PartnerID),
		toBoolString(e.Paying),
		toJsonVarchar(e.Plan),
		toVarchar(e.Platform),
		parseTime(e.ProcessedTime),
		toVarchar(e.Region),
		toFloat64String(e.SampleRate),
		toInterfaceVarchar(e.Schema),
		parseTime(e.ServerReceivedTime),
		parseTime(e.ServerUploadTime),
		e.SessionID,
		toInterfaceVarchar(e.SourceID),
		toVarchar(e.StartVersion),
		parseTime(e.UserCreationTime),
		toVarchar(e.UserID),
		toJsonVarchar(e.UserProperties),
		toVarchar(e.UUID),
		toVarchar(e.VersionName),
	)
}

func (e *Event) String() string {
	return fmt.Sprintf("Event{Adid: %s, AmplitudeAttributionIDs: %v, AmplitudeEventType: %v, AmplitudeID: %d, App: %d, City: %s, ClientEventTime: %s, ClientUploadTime: %s, Country: %s, Data: %v, DataType: %s, DeviceBrand: %s, DeviceCarrier: %s, DeviceFamily: %s, DeviceID: %s, DeviceManufacturer: %s, DeviceModel: %s, DeviceType: %s, DMA: %s, EventID: %d, EventProperties: %v, EventTime: %s, EventType: %s, GlobalUserProperties: %v, GroupProperties: %v, Groups: %v, IDFA: %s, InsertID: %s, InsertKey: %v, IPAddress: %s, IsAttributionEvent: %v, Language: %s, Library: %s, LocationLat: %v, LocationLng: %v, OSName: %s, OSVersion: %s, PartnerID: %v, Paying: %v, Plan: %v, Platform: %s, ProcessedTime: %s, Region: %s, SampleRate: %v, Schema: %v, ServerReceivedTime: %s, ServerUploadTime: %s, SessionID: %d, SourceID: %v, StartVersion: %s, UserCreationTime: %s, UserID: %s, UserProperties: %v, UUID: %s, VersionName: %s}",
		e.Adid, e.AmplitudeAttributionIDs, e.AmplitudeEventType, e.AmplitudeID, e.App, e.City, e.ClientEventTime, e.ClientUploadTime, e.Country, e.Data, e.DataType, e.DeviceBrand, e.DeviceCarrier, e.DeviceFamily, e.DeviceID, e.DeviceManufacturer, e.DeviceModel, e.DeviceType, e.DMA, e.EventID, e.EventProperties, e.EventTime, e.EventType, e.GlobalUserProperties, e.GroupProperties, e.Groups, e.IDFA, e.InsertID, e.InsertKey, e.IPAddress, e.IsAttributionEvent, e.Language, e.Library, e.LocationLat, e.LocationLng, e.OSName, e.OSVersion, e.PartnerID, e.Paying, e.Plan, e.Platform, e.ProcessedTime, e.Region, e.SampleRate, e.Schema, e.ServerReceivedTime, e.ServerUploadTime, e.SessionID, e.SourceID, e.StartVersion, e.UserCreationTime, e.UserID, e.UserProperties, e.UUID, e.VersionName)
}

func EventsTableSchema() string {
	return `(
		"adid" VARCHAR,
		"amplitude_attribution_ids" VARCHAR,
		"amplitude_event_type" VARCHAR,
		"amplitude_id" BIGINT,
		"app" INTEGER,
		"city" VARCHAR,
		"client_event_time" TIMESTAMP(6),
		"client_upload_time" TIMESTAMP(6),
		"country" VARCHAR,
		"data" VARCHAR,
		"data_type" VARCHAR,
		"device_brand" VARCHAR,
		"device_carrier" VARCHAR,
		"device_family" VARCHAR,
		"device_id" VARCHAR,
		"device_manufacturer" VARCHAR,
		"device_model" VARCHAR,
		"device_type" VARCHAR,
		"dma" VARCHAR,
		"event_id" BIGINT,
		"event_properties" VARCHAR,
		"event_time" TIMESTAMP(6),
		"event_type" VARCHAR,
		"global_user_properties" VARCHAR,
		"group_properties" VARCHAR,
		"groups" VARCHAR,
		"idfa" VARCHAR,
		"insert_id" VARCHAR,
		"insert_key" VARCHAR,
		"ip_address" VARCHAR,
		"is_attribution_event" BOOLEAN,
		"language" VARCHAR,
		"library" VARCHAR,
		"location_lat" DOUBLE,
		"location_lng" DOUBLE,
		"os_name" VARCHAR,
		"os_version" VARCHAR,
		"partner_id" VARCHAR,
		"paying" BOOLEAN,
		"plan" VARCHAR,
		"platform" VARCHAR,
		"processed_time" TIMESTAMP(6),
		"region" VARCHAR,
		"sample_rate" DOUBLE,
		"schema" VARCHAR,
		"server_received_time" TIMESTAMP(6),
		"server_upload_time" TIMESTAMP(6),
		"session_id" BIGINT,
		"source_id" VARCHAR,
		"start_version" VARCHAR,
		"user_creation_time" TIMESTAMP(6),
		"user_id" VARCHAR,
		"user_properties" VARCHAR,
		"uuid" VARCHAR,
		"version_name" VARCHAR
	)`
}

func QuotedEventsTableColumnNames() string {
	return strings.Join([]string{
		"adid",
		"amplitude_attribution_ids",
		"amplitude_event_type",
		"amplitude_id",
		"app",
		"city",
		"client_event_time",
		"client_upload_time",
		"country",
		"data",
		"data_type",
		"device_brand",
		"device_carrier",
		"device_family",
		"device_id",
		"device_manufacturer",
		"device_model",
		"device_type",
		"dma",
		"event_id",
		"event_properties",
		"event_time",
		"event_type",
		"global_user_properties",
		"group_properties",
		"groups",
		"idfa",
		"insert_id",
		"insert_key",
		"ip_address",
		"is_attribution_event",
		"language",
		"library",
		"location_lat",
		"location_lng",
		"os_name",
		"os_version",
		"partner_id",
		"paying",
		"plan",
		"platform",
		"processed_time",
		"region",
		"sample_rate",
		"schema",
		"server_received_time",
		"server_upload_time",
		"session_id",
		"source_id",
		"start_version",
		"user_creation_time",
		"user_id",
		"user_properties",
		"uuid",
		"version_name",
	}, ",")
}
