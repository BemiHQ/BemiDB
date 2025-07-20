package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserSelect struct {
	config *Config
	utils  *ParserUtils
}

func NewParserSelect(config *Config) *ParserSelect {
	return &ParserSelect{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserSelect) SetDefaultTargetName(targetNode *pgQuery.Node, name string) {
	target := targetNode.GetResTarget()

	if target.Name == "" {
		target.Name = name
	}
}
