package attio

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type RecordDeal struct {
	Values struct {
		Id                []Value             `json:"record_id"`
		Name              []Value             `json:"name"`
		Stage             []StatusValue       `json:"stage"`
		Owner             []UserValue         `json:"owner"`
		Value             []CurrencyValue     `json:"value"`
		AssociatedPeople  []RelationshipValue `json:"associated_people"`
		AssociatedCompany []RelationshipValue `json:"associated_company"`
		CreatedAt         []Value             `json:"created_at"`
		CreatedBy         []UserValue         `json:"created_by"`
	} `json:"values"`
}

func (record *RecordDeal) ToMap(parser *Parser) map[string]interface{} {
	result := make(map[string]interface{})

	result["id"] = parser.FirstValue(record.Values.Id)
	result["name"] = parser.FirstValue(record.Values.Name)
	result["stage"] = parser.FirstStatusValue(record.Values.Stage)
	result["owner"] = parser.FirstUserValue(record.Values.Owner)
	result["value"] = parser.FirstCurrencyValue(record.Values.Value)
	result["associated_people"] = parser.AllRelationshipIds(record.Values.AssociatedPeople)
	result["associated_company"] = parser.FirstRelationshipId(record.Values.AssociatedCompany)
	result["created_at"] = parser.FirstValue(record.Values.CreatedAt)
	result["created_by"] = parser.FirstUserValue(record.Values.CreatedBy)

	return result
}

func DealsIcebergSchemaColumns(config *common.CommonConfig) []*common.IcebergSchemaColumn {
	return []*common.IcebergSchemaColumn{
		{Config: config, ColumnName: "id", ColumnType: common.IcebergColumnTypeString, Position: 1},
		{Config: config, ColumnName: "name", ColumnType: common.IcebergColumnTypeString, Position: 2},
		{Config: config, ColumnName: "stage", ColumnType: common.IcebergColumnTypeString, Position: 3},
		{Config: config, ColumnName: "owner", ColumnType: common.IcebergColumnTypeString, Position: 4},
		{Config: config, ColumnName: "value", ColumnType: common.IcebergColumnTypeFloat, Position: 5},
		{Config: config, ColumnName: "associated_people", ColumnType: common.IcebergColumnTypeString, IsList: true, Position: 6},
		{Config: config, ColumnName: "associated_company", ColumnType: common.IcebergColumnTypeString, Position: 7},
		{Config: config, ColumnName: "created_at", ColumnType: common.IcebergColumnTypeTimestamp, Position: 8},
		{Config: config, ColumnName: "created_by", ColumnType: common.IcebergColumnTypeString, Position: 9},
	}
}
