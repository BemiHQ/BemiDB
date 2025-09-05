package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type RecordCompany struct {
	Values struct {
		Id                                []Value             `json:"record_id"`
		Name                              []Value             `json:"name"`
		Description                       []Value             `json:"description"`
		Domains                           []DomainValue       `json:"domains"`
		Team                              []RelationshipValue `json:"team"`
		Categories                        []SelectValue       `json:"categories"`
		PrimaryLocation                   []LocationValue     `json:"primary_location"`
		LogoUrl                           []Value             `json:"logo_url"`
		Angellist                         []Value             `json:"angellist"`
		Facebook                          []Value             `json:"facebook"`
		Instagram                         []Value             `json:"instagram"`
		Linkedin                          []Value             `json:"linkedin"`
		Twitter                           []Value             `json:"twitter"`
		TwitterFollowerCount              []Value             `json:"twitter_follower_count"`
		EstimatedArrUsd                   []SelectValue       `json:"estimated_arr_usd"`
		FundingRaisedUsd                  []CurrencyValue     `json:"funding_raised_usd"`
		FoundationDate                    []Value             `json:"foundation_date"`
		EmployeeRange                     []SelectValue       `json:"employee_range"`
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
		AssociatedWorkspaces              []RelationshipValue `json:"associated_workspaces"`
		CreatedAt                         []Value             `json:"created_at"`
		CreatedBy                         []UserValue         `json:"created_by"`
	} `json:"values"`
}

func (record *RecordCompany) ToMap(parser *Parser) map[string]interface{} {
	result := make(map[string]interface{})

	result["id"] = parser.FirstValue(record.Values.Id)
	result["name"] = parser.FirstValue(record.Values.Name)
	result["description"] = parser.FirstValue(record.Values.Description)
	result["domains"] = parser.AllDomainValues(record.Values.Domains)
	result["team"] = parser.AllRelationshipIds(record.Values.Team)
	result["categories"] = parser.AllSelectValues(record.Values.Categories)
	result["primary_location"] = parser.FirstLocationValue(record.Values.PrimaryLocation)
	result["logo_url"] = parser.FirstValue(record.Values.LogoUrl)
	result["angellist"] = parser.FirstValue(record.Values.Angellist)
	result["facebook"] = parser.FirstValue(record.Values.Facebook)
	result["instagram"] = parser.FirstValue(record.Values.Instagram)
	result["linkedin"] = parser.FirstValue(record.Values.Linkedin)
	result["twitter"] = parser.FirstValue(record.Values.Twitter)
	result["twitter_follower_count"] = parser.FirstValue(record.Values.TwitterFollowerCount)
	result["estimated_arr_usd"] = parser.FirstSelectValue(record.Values.EstimatedArrUsd)
	result["funding_raised_usd"] = parser.FirstCurrencyValue(record.Values.FundingRaisedUsd)
	result["foundation_date"] = parser.FirstValue(record.Values.FoundationDate)
	result["employee_range"] = parser.FirstSelectValue(record.Values.EmployeeRange)
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
	result["associated_workspaces"] = parser.AllRelationshipIds(record.Values.AssociatedWorkspaces)
	result["created_at"] = parser.FirstValue(record.Values.CreatedAt)
	result["created_by"] = parser.FirstUserValue(record.Values.CreatedBy)

	return result
}

func CompaniesIcebergSchemaColumns(config *common.CommonConfig) []*common.IcebergSchemaColumn {
	return []*common.IcebergSchemaColumn{
		{Config: config, ColumnName: "id", ColumnType: common.IcebergColumnTypeString, Position: 1},
		{Config: config, ColumnName: "name", ColumnType: common.IcebergColumnTypeString, Position: 2},
		{Config: config, ColumnName: "description", ColumnType: common.IcebergColumnTypeString, Position: 3},
		{Config: config, ColumnName: "domains", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 4},
		{Config: config, ColumnName: "team", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 5},
		{Config: config, ColumnName: "categories", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 6},
		{Config: config, ColumnName: "primary_location", ColumnType: common.IcebergColumnTypeString, Position: 7},
		{Config: config, ColumnName: "logo_url", ColumnType: common.IcebergColumnTypeString, Position: 8},
		{Config: config, ColumnName: "angellist", ColumnType: common.IcebergColumnTypeString, Position: 9},
		{Config: config, ColumnName: "facebook", ColumnType: common.IcebergColumnTypeString, Position: 10},
		{Config: config, ColumnName: "instagram", ColumnType: common.IcebergColumnTypeString, Position: 11},
		{Config: config, ColumnName: "linkedin", ColumnType: common.IcebergColumnTypeString, Position: 12},
		{Config: config, ColumnName: "twitter", ColumnType: common.IcebergColumnTypeString, Position: 13},
		{Config: config, ColumnName: "twitter_follower_count", ColumnType: common.IcebergColumnTypeInteger, Position: 14},
		{Config: config, ColumnName: "estimated_arr_usd", ColumnType: common.IcebergColumnTypeString, Position: 15},
		{Config: config, ColumnName: "funding_raised_usd", ColumnType: common.IcebergColumnTypeFloat, Position: 16},
		{Config: config, ColumnName: "foundation_date", ColumnType: common.IcebergColumnTypeDate, Position: 17},
		{Config: config, ColumnName: "employee_range", ColumnType: common.IcebergColumnTypeString, Position: 18},
		{Config: config, ColumnName: "first_calendar_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 19},
		{Config: config, ColumnName: "last_calendar_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 20},
		{Config: config, ColumnName: "next_calendar_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 21},
		{Config: config, ColumnName: "first_email_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 22},
		{Config: config, ColumnName: "last_email_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 23},
		{Config: config, ColumnName: "first_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 24},
		{Config: config, ColumnName: "last_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 25},
		{Config: config, ColumnName: "next_interaction", ColumnType: common.IcebergColumnTypeTimestamp, Position: 26},
		{Config: config, ColumnName: "strongest_connection_strength_legacy", ColumnType: common.IcebergColumnTypeInteger, Position: 27},
		{Config: config, ColumnName: "strongest_connection_strength", ColumnType: common.IcebergColumnTypeString, Position: 28},
		{Config: config, ColumnName: "strongest_connection_user", ColumnType: common.IcebergColumnTypeString, Position: 29},
		{Config: config, ColumnName: "associated_deals", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 30},
		{Config: config, ColumnName: "associated_workspaces", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 31},
		{Config: config, ColumnName: "created_at", ColumnType: common.IcebergColumnTypeTimestamp, Position: 32},
		{Config: config, ColumnName: "created_by", ColumnType: common.IcebergColumnTypeString, Position: 33},
	}
}
