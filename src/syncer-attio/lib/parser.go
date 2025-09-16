package attio

import (
	"strings"
)

type Value struct {
	Value interface{} `json:"value"`
}
type DomainValue struct {
	Value string `json:"domain"`
}
type RelationshipValue struct {
	Value string `json:"target_record_id"`
}
type SelectValue struct {
	Option struct {
		Value string `json:"title"`
	} `json:"option"`
}
type CurrencyValue struct {
	Value float64 `json:"currency_value"`
}
type InteractionValue struct {
	Value string `json:"interacted_at"`
}
type UserValue struct {
	Value string `json:"referenced_actor_id"`
}
type StatusValue struct {
	Status struct {
		Value string `json:"title"`
	} `json:"status"`
}
type PhoneNumberValue struct {
	Value string `json:"phone_number"`
}
type EmailValue struct {
	Value string `json:"email_address"`
}
type NameValue struct {
	Value string `json:"full_name"`
}

type LocationValue struct {
	Line1    string `json:"line_1"`
	Line2    string `json:"line_2"`
	Line3    string `json:"line_3"`
	Line4    string `json:"line_4"`
	Locality string `json:"locality"`
	Region   string `json:"region"`
	Postcode string `json:"postcode"`
	Country  string `json:"country_code"`
}

type Parser struct {
	Config *Config
}

func NewParser(config *Config) *Parser {
	return &Parser{
		Config: config,
	}
}

func (parser *Parser) FirstValue(values []Value) interface{} {
	if len(values) > 0 {
		return values[0].Value
	}
	return nil
}

func (parser *Parser) FirstCurrencyValue(values []CurrencyValue) interface{} {
	if len(values) > 0 {
		return values[0].Value
	}
	return nil
}

func (parser *Parser) FirstInteractionValue(values []InteractionValue) interface{} {
	if len(values) > 0 {
		return values[0].Value
	}
	return nil
}

func (parser *Parser) FirstUserValue(values []UserValue) interface{} {
	if len(values) > 0 {
		return values[0].Value
	}
	return nil
}

func (parser *Parser) FirstSelectValue(values []SelectValue) interface{} {
	if len(values) > 0 {
		return values[0].Option.Value
	}
	return nil
}

func (parser *Parser) FirstStatusValue(values []StatusValue) interface{} {
	if len(values) > 0 {
		return values[0].Status.Value
	}
	return nil
}

func (parser *Parser) FirstRelationshipId(values []RelationshipValue) interface{} {
	if len(values) > 0 {
		return values[0].Value
	}
	return nil
}

func (parser *Parser) FirstNameValue(values []NameValue) interface{} {
	if len(values) > 0 {
		return values[0].Value
	}
	return nil
}

func (parser *Parser) FirstLocationValue(values []LocationValue) interface{} {
	if len(values) > 0 {
		location := values[0]
		parts := []string{}
		if location.Line1 != "" {
			parts = append(parts, location.Line1)
		}
		if location.Line2 != "" {
			parts = append(parts, location.Line2)
		}
		if location.Line3 != "" {
			parts = append(parts, location.Line3)
		}
		if location.Line4 != "" {
			parts = append(parts, location.Line4)
		}
		if location.Locality != "" {
			parts = append(parts, location.Locality)
		}
		if location.Region != "" {
			parts = append(parts, location.Region)
		}
		if location.Postcode != "" {
			parts = append(parts, location.Postcode)
		}
		if location.Country != "" {
			parts = append(parts, location.Country)
		}
		return strings.Join(parts, ", ")
	}
	return nil
}

func (parser *Parser) AllDomainValues(values []DomainValue) []string {
	domains := []string{}
	for _, value := range values {
		domains = append(domains, value.Value)
	}
	return domains
}

func (parser *Parser) AllRelationshipIds(values []RelationshipValue) []string {
	ids := []string{}
	for _, value := range values {
		ids = append(ids, value.Value)
	}
	return ids
}

func (parser *Parser) AllSelectValues(values []SelectValue) []string {
	options := []string{}
	for _, value := range values {
		options = append(options, value.Option.Value)
	}
	return options
}

func (parser *Parser) AllEmailValues(values []EmailValue) []string {
	emails := []string{}
	for _, value := range values {
		emails = append(emails, value.Value)
	}
	return emails
}

func (parser *Parser) AllPhoneNumberValues(values []PhoneNumberValue) []string {
	phones := []string{}
	for _, value := range values {
		phones = append(phones, value.Value)
	}
	return phones
}
