package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type RecordPerson struct {
	Values struct {
		Id                                []Value             `json:"record_id"`
		Name                              []NameValue         `json:"name"`
		Description                       []Value             `json:"description"`
		EmailAddresses                    []EmailValue        `json:"email_addresses"`
		Company                           []RelationshipValue `json:"company"`
		JobTitle                          []Value             `json:"job_title"`
		AvatarUrl                         []Value             `json:"avatar_url"`
		PhoneNumbers                      []PhoneNumberValue  `json:"phone_numbers"`
		PrimaryLocation                   []LocationValue     `json:"primary_location"`
		Angellist                         []Value             `json:"angellist"`
		Facebook                          []Value             `json:"facebook"`
		Instagram                         []Value             `json:"instagram"`
		Linkedin                          []Value             `json:"linkedin"`
		Twitter                           []Value             `json:"twitter"`
		TwitterFollowerCount              []Value             `json:"twitter_follower_count"`
		FirstCalendarInteraction          []InteractionValue  `json:"first_calendar_interaction"`
		LastCalendarInteraction           []InteractionValue  `json:"last_calendar_interaction"`
		NextCalendarInteraction           []InteractionValue  `json:"next_calendar_interaction"`
		FirstEmailInteraction             []InteractionValue  `json:"first_email_interaction"`
		LastEmailInteraction              []InteractionValue  `json:"last_email_interaction"`
		FirstInteraction                  []InteractionValue  `json:"first_interaction"`
		LastInteraction                   []InteractionValue  `json:"last_interaction"`
		NextInteraction                   []InteractionValue  `json:"next_interaction"`
		StrongestConnectionStrengthLegacy []Value             `json:"strongest_connection_strength_legacy"`
		StrongestConnectionStrength       []SelectValue       `json:"strongest_connection_strength"`
		StrongestConnectionUser           []UserValue         `json:"strongest_connection_user"`
		AssociatedDeals                   []RelationshipValue `json:"associated_deals"`
		AssociatedUsers                   []RelationshipValue `json:"associated_users"`
		CreatedAt                         []Value             `json:"created_at"`
		CreatedBy                         []UserValue         `json:"created_by"`
	} `json:"values"`
}

func (record *RecordPerson) ToMap(parser *Parser) map[string]interface{} {
	result := make(map[string]interface{})

	result["id"] = parser.FirstValue(record.Values.Id)
	result["name"] = parser.FirstNameValue(record.Values.Name)
	result["description"] = parser.FirstValue(record.Values.Description)
	result["email_addresses"] = parser.AllEmailValues(record.Values.EmailAddresses)
	result["company"] = parser.FirstRelationshipId(record.Values.Company)
	result["job_title"] = parser.FirstValue(record.Values.JobTitle)
	result["avatar_url"] = parser.FirstValue(record.Values.AvatarUrl)
	result["phone_numbers"] = parser.AllPhoneNumberValues(record.Values.PhoneNumbers)
	result["primary_location"] = parser.FirstLocationValue(record.Values.PrimaryLocation)
	result["angellist"] = parser.FirstValue(record.Values.Angellist)
	result["facebook"] = parser.FirstValue(record.Values.Facebook)
	result["instagram"] = parser.FirstValue(record.Values.Instagram)
	result["linkedin"] = parser.FirstValue(record.Values.Linkedin)
	result["twitter"] = parser.FirstValue(record.Values.Twitter)
	result["twitter_follower_count"] = parser.FirstValue(record.Values.TwitterFollowerCount)
	result["first_calendar_interaction"] = parser.FirstInteractionValue(record.Values.FirstCalendarInteraction)
	result["last_calendar_interaction"] = parser.FirstInteractionValue(record.Values.LastCalendarInteraction)
	result["next_calendar_interaction"] = parser.FirstInteractionValue(record.Values.NextCalendarInteraction)
	result["first_email_interaction"] = parser.FirstInteractionValue(record.Values.FirstEmailInteraction)
	result["last_email_interaction"] = parser.FirstInteractionValue(record.Values.LastEmailInteraction)
	result["first_interaction"] = parser.FirstInteractionValue(record.Values.FirstInteraction)
	result["last_interaction"] = parser.FirstInteractionValue(record.Values.LastInteraction)
	result["next_interaction"] = parser.FirstInteractionValue(record.Values.NextInteraction)
	result["strongest_connection_strength_legacy"] = parser.FirstValue(record.Values.StrongestConnectionStrengthLegacy)
	result["strongest_connection_strength"] = parser.FirstSelectValue(record.Values.StrongestConnectionStrength)
	result["strongest_connection_user"] = parser.FirstUserValue(record.Values.StrongestConnectionUser)
	result["associated_deals"] = parser.AllRelationshipIds(record.Values.AssociatedDeals)
	result["associated_users"] = parser.AllRelationshipIds(record.Values.AssociatedUsers)
	result["created_at"] = parser.FirstValue(record.Values.CreatedAt)
	result["created_by"] = parser.FirstUserValue(record.Values.CreatedBy)

	return result
}

func PeopleIcebergSchemaColumns(config *common.CommonConfig) []*common.IcebergSchemaColumn {
	return []*common.IcebergSchemaColumn{
		{Config: config, ColumnName: "id", ColumnType: common.IcebergColumnTypeString, Position: 1},
		{Config: config, ColumnName: "name", ColumnType: common.IcebergColumnTypeString, Position: 2},
		{Config: config, ColumnName: "description", ColumnType: common.IcebergColumnTypeString, Position: 3},
		{Config: config, ColumnName: "email_addresses", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 4},
		{Config: config, ColumnName: "company", ColumnType: common.IcebergColumnTypeString, Position: 5},
		{Config: config, ColumnName: "job_title", ColumnType: common.IcebergColumnTypeString, Position: 6},
		{Config: config, ColumnName: "avatar_url", ColumnType: common.IcebergColumnTypeString, Position: 7},
		{Config: config, ColumnName: "phone_numbers", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 8},
		{Config: config, ColumnName: "primary_location", ColumnType: common.IcebergColumnTypeString, Position: 9},
		{Config: config, ColumnName: "angellist", ColumnType: common.IcebergColumnTypeString, Position: 10},
		{Config: config, ColumnName: "facebook", ColumnType: common.IcebergColumnTypeString, Position: 11},
		{Config: config, ColumnName: "instagram", ColumnType: common.IcebergColumnTypeString, Position: 12},
		{Config: config, ColumnName: "linkedin", ColumnType: common.IcebergColumnTypeString, Position: 13},
		{Config: config, ColumnName: "twitter", ColumnType: common.IcebergColumnTypeString, Position: 14},
		{Config: config, ColumnName: "twitter_follower_count", ColumnType: common.IcebergColumnTypeInteger, Position: 15},
		{Config: config, ColumnName: "first_calendar_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 16},
		{Config: config, ColumnName: "last_calendar_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 17},
		{Config: config, ColumnName: "next_calendar_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 18},
		{Config: config, ColumnName: "first_email_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 19},
		{Config: config, ColumnName: "last_email_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 20},
		{Config: config, ColumnName: "first_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 21},
		{Config: config, ColumnName: "last_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 22},
		{Config: config, ColumnName: "next_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 23},
		{Config: config, ColumnName: "strongest_connection_strength_legacy", ColumnType: common.IcebergColumnTypeInteger, Position: 24},
		{Config: config, ColumnName: "strongest_connection_strength", ColumnType: common.IcebergColumnTypeString, Position: 25},
		{Config: config, ColumnName: "strongest_connection_user", ColumnType: common.IcebergColumnTypeString, Position: 26},
		{Config: config, ColumnName: "associated_deals", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 27},
		{Config: config, ColumnName: "associated_users", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 28},
		{Config: config, ColumnName: "created_at", ColumnType: common.IcebergColumnTypeTimestamp, Position: 29},
		{Config: config, ColumnName: "created_by", ColumnType: common.IcebergColumnTypeString, Position: 30},
	}
}
